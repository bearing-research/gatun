package org.gatun.server;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * The tables a session's client has lent to Java: each is read where the client wrote it, in the
 * session's shared memory, until the client takes it back.
 *
 * <p>A lent table is an ordinary VectorSchemaRoot, and Java code may hand it to any thread and keep
 * it for as long as it likes. Two things keep that from reading memory that is no longer what it
 * was:
 *
 * <ul>
 *   <li>The session's own mapping of its memory is confined to the session's thread and unmapped
 *       the moment the session ends. Lent tables go through a second mapping of the same file,
 *       which stays until the session has ended and none of their buffers is referenced.
 *   <li>When a loan ends the client reuses the bytes. From then on the table's buffers report a
 *       reference count of zero ({@link SharedMemoryBuffers#revoke}), which makes every checked
 *       Arrow accessor throw. A raw address taken earlier, or a JVM with Arrow's bounds checks
 *       switched off, is not covered.
 * </ul>
 */
final class ArrowLoans {
  private static final Logger LOG = Logger.getLogger(ArrowLoans.class.getName());

  private final long fileSize;
  // Where the payload zone starts in the file: the offsets of loans count from there
  private final long zoneOffset;
  private final BufferAllocator allocator;
  private final SessionQuota quota;

  // All guarded by this
  private FileChannel file;
  private Arena arena;
  private MemorySegment mapping;
  private final Set<Loan> open = new HashSet<>();
  private int referenced; // loans, open or ended, with a buffer that is still referenced
  private boolean closed;

  /**
   * @param quota charged for what a lent table does take from the allocator
   */
  ArrowLoans(long fileSize, long zoneOffset, BufferAllocator allocator, SessionQuota quota) {
    this.fileSize = fileSize;
    this.zoneOffset = zoneOffset;
    this.allocator = allocator;
    this.quota = quota;
  }

  /**
   * The session's shared memory file, which the first loan maps. The open file and not its path:
   * the path can come to name another file, or none, while the session lasts.
   */
  synchronized void file(FileChannel file) {
    this.file = file;
  }

  /** One lent table. */
  final class Loan {
    private final VectorSchemaRoot table;
    private final SharedMemoryBuffers buffers; // null for a table without a record batch
    private final long start;
    private final long end;
    private final long allocated; // what the table took from the allocator, held in the quota
    private boolean ended; // guarded by ArrowLoans.this

    private Loan(
        VectorSchemaRoot table, SharedMemoryBuffers buffers, long start, long end, long allocated) {
      this.table = table;
      this.buffers = buffers;
      this.start = start;
      this.end = end;
      this.allocated = allocated;
    }

    VectorSchemaRoot table() {
      return table;
    }

    /** Whether the table has been taken back (and closed). */
    boolean ended() {
      synchronized (ArrowLoans.this) {
        return ended;
      }
    }

    /** Take the table back: it cannot be read from here on, and its bytes are the client's. */
    void end() {
      synchronized (ArrowLoans.this) {
        if (ended) {
          return;
        }
        ended = true;
        open.remove(this);
      }
      if (buffers != null) {
        buffers.revoke();
      }
      try {
        table.close();
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "Failed to close a lent Arrow table", e);
      }
      quota.releaseHeldArrowBytes(allocated);
      if (buffers != null && buffers.references() > 0) {
        LOG.warning(
            "Java code still holds buffers of an Arrow table that Python lent it and has taken"
                + " back. Reading them fails from here on; release them, or copy what is needed"
                + " while the loan lasts.");
      }
    }
  }

  /**
   * Build a table over the Arrow IPC stream at {@code offset} in the payload zone, without copying
   * it. The stream must hold one record batch at most: several would have to be stitched together,
   * which is a copy.
   */
  synchronized Loan lend(long offset, long length) throws IOException {
    if (closed) {
      throw new IllegalStateException("The session has ended");
    }
    MemorySegment stream = mapping().asSlice(zoneOffset + offset, length);
    List<ArrowMemoryHandler.IpcMessage> messages = ArrowMemoryHandler.ipcMessages(stream);
    Schema schema = ArrowMemoryHandler.schemaOf(messages);
    if (messages.size() > 2) {
      throw new IllegalArgumentException(
          "A table is lent as one record batch; the stream holds "
              + (messages.size() - 1)
              + " messages after its schema");
    }

    VectorSchemaRoot table = VectorSchemaRoot.create(schema, allocator);
    SharedMemoryBuffers buffers = null;
    long allocated;
    try {
      if (messages.size() == 2) {
        ArrowMemoryHandler.IpcMessage message = messages.get(1);
        if (message.metadata().headerType() != MessageHeader.RecordBatch) {
          throw new IllegalArgumentException(
              "Expected a record batch after the schema, found "
                  + MessageHeader.name(message.metadata().headerType()));
        }
        RecordBatch header = (RecordBatch) message.metadata().header(new RecordBatch());
        if (header.compression() != null) {
          throw new IllegalArgumentException(
              "A compressed record batch cannot be read in place; send it uncompressed");
        }
        buffers = new SharedMemoryBuffers(message.bodyLength(), allocator, this::returned);
        referenced++;
        ArrowBuf body =
            new ArrowBuf(
                buffers, null, message.bodyLength(), stream.address() + message.bodyStart());
        ArrowRecordBatch batch;
        try {
          // Takes over the reference the body starts with, but only if it succeeds
          batch = MessageSerializer.deserializeRecordBatch(header, body);
        } catch (Throwable t) {
          buffers.abandon();
          throw t;
        }
        try (batch) {
          new VectorLoader(table).load(batch);
        }
      } else {
        // A stream with a schema and no batch is a table without rows
        table.setRowCount(0);
      }
      allocated = allocatedBy(table, buffers);
      quota.holdArrowBytes(allocated);
    } catch (Throwable t) {
      table.close();
      throw t;
    }
    Loan loan = new Loan(table, buffers, offset, offset + length, allocated);
    open.add(loan);
    return loan;
  }

  /**
   * What a lent table took from the allocator. Its data is the client's, but a stream leaves out
   * the validity bitmap of a column without nulls, and Arrow makes one when it loads the column:
   * one bit a row, which for a column of booleans is as much as the data.
   */
  private static long allocatedBy(VectorSchemaRoot table, SharedMemoryBuffers lent) {
    IdentityHashMap<ReferenceManager, Boolean> counted = new IdentityHashMap<>();
    long bytes = 0;
    for (FieldVector vector : table.getFieldVectors()) {
      // Includes the buffers of a nested vector's children
      for (ArrowBuf buffer : vector.getBuffers(false)) {
        ReferenceManager manager = buffer.getReferenceManager();
        if (manager != lent && counted.put(manager, true) == null) {
          bytes += manager.getAccountedSize();
        }
      }
    }
    return bytes;
  }

  /** One past the last byte of the payload zone that an open loan covers; 0 with none open. */
  synchronized long end() {
    long end = 0;
    for (Loan loan : open) {
      end = Math.max(end, loan.end);
    }
    return end;
  }

  /** Whether writing the payload zone from {@code from} up to {@code to} would hit a loan. */
  synchronized boolean overlaps(long from, long to) {
    for (Loan loan : open) {
      if (from < loan.end && to > loan.start) {
        return true;
      }
    }
    return false;
  }

  /** End every open loan: the client is about to reuse the whole zone. */
  void endAll() {
    List<Loan> loans;
    synchronized (this) {
      loans = new ArrayList<>(open);
    }
    for (Loan loan : loans) {
      loan.end();
    }
  }

  /**
   * The session has ended. The mapping goes now if nothing refers to it, and otherwise with the
   * last buffer that Java code still holds.
   */
  void close() {
    endAll();
    synchronized (this) {
      closed = true;
      if (referenced == 0) {
        unmap();
      } else {
        LOG.warning(
            "A session ended while Java code still holds buffers of "
                + referenced
                + " Arrow table(s) that its client had lent. The session's shared memory stays"
                + " mapped until they are released.");
      }
    }
  }

  private synchronized void returned() {
    referenced--;
    if (closed && referenced == 0) {
      unmap();
    }
  }

  private MemorySegment mapping() throws IOException {
    if (mapping == null) {
      if (file == null) {
        throw new IllegalStateException("The session's shared memory is not open");
      }
      // Shared: the tables are read by whatever thread Java code passes them to
      Arena shared = Arena.ofShared();
      try {
        // Writable, though nothing here writes: Java code that sets a value in a lent table
        // must change the client's bytes, not fault
        mapping = file.map(FileChannel.MapMode.READ_WRITE, 0, fileSize, shared);
      } catch (IOException | RuntimeException e) {
        shared.close();
        throw e;
      }
      arena = shared;
    }
    return mapping;
  }

  private void unmap() {
    if (arena != null) {
      try {
        arena.close();
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "Could not unmap the memory of lent Arrow tables", e);
      }
      arena = null;
      mapping = null;
    }
  }
}
