package bench;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

/**
 * JVM half of benchmark_transport.py: the socket and file baselines that gatun's shared memory
 * paths are measured against, written with stock Arrow Java and nothing from gatun.
 *
 * <p>Every reader returns {rows, sum of the id column, data bytes read by the scan}.
 */
public final class TransportBench {
  private static final BufferAllocator ALLOC = new RootAllocator(Long.MAX_VALUE);

  private static ServerSocketChannel readerServer;
  private static Thread readerThread;
  private static volatile long[] readerResult;
  private static volatile Throwable readerError;

  private TransportBench() {}

  /** A table of (id int64, value float64, s 16-byte string), 36 bytes of data a row. */
  public static VectorSchemaRoot make(long rows) {
    int n = Math.toIntExact(rows);
    BigIntVector id = new BigIntVector("id", ALLOC);
    Float8Vector value = new Float8Vector("value", ALLOC);
    VarCharVector s = new VarCharVector("s", ALLOC);
    id.allocateNew(n);
    value.allocateNew(n);
    s.allocateNew(16L * n, n);
    byte[] text = new byte[16];
    for (int i = 0; i < n; i++) {
      id.set(i, i);
      value.set(i, i * 0.5);
      int v = i;
      for (int k = 15; k >= 0; k--) {
        text[k] = (byte) ('0' + v % 10);
        v /= 10;
      }
      s.set(i, text);
    }
    id.setValueCount(n);
    value.setValueCount(n);
    s.setValueCount(n);
    return new VectorSchemaRoot(List.<FieldVector>of(id, value, s));
  }

  /** Read every data byte of the table. Returns {rows, id sum, bytes read}. */
  public static long[] checksum(VectorSchemaRoot root) {
    long[] acc = new long[3];
    scanInto(root, acc);
    return acc;
  }

  private static void scanInto(VectorSchemaRoot root, long[] acc) {
    int rows = root.getRowCount();
    acc[0] += rows;
    for (FieldVector v : root.getFieldVectors()) {
      ArrowBuf data = v.getDataBuffer();
      long bytes;
      if (v instanceof VarCharVector vc) {
        bytes = rows == 0 ? 0 : vc.getOffsetBuffer().getInt(4L * rows);
      } else {
        bytes = 8L * rows;
      }
      long sum = 0;
      long i = 0;
      for (; i + 8 <= bytes; i += 8) {
        sum += data.getLong(i);
      }
      for (; i < bytes; i++) {
        sum += data.getByte(i);
      }
      if (v.getName().equals("id")) {
        acc[1] += sum;
      }
      acc[2] += bytes;
    }
  }

  // ---- Python -> Java baselines -------------------------------------------------------------

  /** Listen on a Unix socket and read one Arrow IPC stream from it on a background thread. */
  public static void startSocketReader(String path, boolean scan) throws IOException {
    Files.deleteIfExists(Path.of(path));
    readerResult = null;
    readerError = null;
    readerServer = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
    readerServer.bind(UnixDomainSocketAddress.of(path));
    ServerSocketChannel server = readerServer;
    readerThread =
        new Thread(
            () -> {
              try (server;
                  SocketChannel ch = server.accept();
                  ArrowStreamReader reader = new ArrowStreamReader(ch, ALLOC)) {
                long[] acc = new long[3];
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                  if (scan) {
                    scanInto(root, acc);
                  } else {
                    acc[0] += root.getRowCount();
                  }
                }
                readerResult = acc;
              } catch (Throwable t) {
                readerError = t;
              }
            },
            "transport-bench-reader");
    readerThread.start();
  }

  /** Wait for the stream started by startSocketReader to be fully read. */
  public static long[] awaitSocketReader() throws Exception {
    readerThread.join();
    if (readerError != null) {
      throw new RuntimeException(readerError);
    }
    return readerResult;
  }

  /** Read an Arrow IPC stream file with the stock reader, which copies into its own buffers. */
  public static long[] readFile(String path, boolean scan) throws IOException {
    long[] acc = new long[3];
    try (FileChannel ch = FileChannel.open(Path.of(path), StandardOpenOption.READ);
        ArrowStreamReader reader = new ArrowStreamReader(ch, ALLOC)) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        if (scan) {
          scanInto(root, acc);
        } else {
          acc[0] += root.getRowCount();
        }
      }
    }
    return acc;
  }

  // ---- Java -> Python baselines -------------------------------------------------------------

  /** Connect to a Unix socket Python is listening on and write the table as an IPC stream. */
  public static void writeSocket(VectorSchemaRoot root, String path) throws IOException {
    try (SocketChannel ch = SocketChannel.open(UnixDomainSocketAddress.of(path));
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, ch)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
  }

  /** Write the table as an IPC stream file (put it on tmpfs for the mmap baseline). */
  public static long writeFile(VectorSchemaRoot root, String path) throws IOException {
    try (FileChannel ch =
            FileChannel.open(
                Path.of(path),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, ch)) {
      writer.start();
      writer.writeBatch();
      writer.end();
      return ch.position();
    }
  }
}
