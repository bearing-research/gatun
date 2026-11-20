package org.gatun.server;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OwnershipTransferResult;
import org.apache.arrow.memory.ReferenceManager;

/**
 * Reference counting for Arrow buffers that lie in a client's shared memory. The memory is the
 * client's and is never freed here; the count says whether Java code can still reach it.
 *
 * <p>Every such buffer uses this one class, whichever way it came (a batch loaded in place, a lent
 * table). Arrow asks for the count before every read of a buffer, at a single call site, and the
 * JIT compiles that call into the reading loop only while it has seen no more than two classes
 * there. Arrow's own manager is one. A second class of ours made a full read of a lent table take
 * half as long again, and after enough such reads slowed the reading of every other table in the
 * JVM.
 */
final class SharedMemoryBuffers implements ReferenceManager {
  private static final String ENDED =
      "This Arrow table was lent by Python, which has taken it back";

  // Added to the count by revoke(). A revoked count is negative, so getRefCount() is one read and
  // one comparison; the references still held are the count less this.
  private static final int REVOKED = Integer.MIN_VALUE / 2;

  private final AtomicInteger count = new AtomicInteger(1);
  private final AtomicBoolean returned = new AtomicBoolean();
  private final long size;
  private final BufferAllocator allocator;
  private final Runnable onReturned;

  /**
   * @param size of the region the buffers lie in
   * @param allocator reported as the buffers' allocator; nothing is taken from it
   * @param onReturned run once, when the last reference is released; may be null
   */
  SharedMemoryBuffers(long size, BufferAllocator allocator, Runnable onReturned) {
    this.size = size;
    this.allocator = allocator;
    this.onReturned = onReturned;
  }

  private static boolean isRevoked(int count) {
    return count < REVOKED / 2;
  }

  /** Zero once revoked: that is what Arrow's accessors check before every read. */
  @Override
  public int getRefCount() {
    return Math.max(count.get(), 0);
  }

  /** The references still held, revoked or not. */
  int references() {
    int n = count.get();
    return isRevoked(n) ? n - REVOKED : n;
  }

  /**
   * Make every buffer unreadable, whatever still refers to them: the memory is about to hold
   * something else. Those references can still be released.
   */
  void revoke() {
    int n;
    do {
      n = count.get();
      if (isRevoked(n)) {
        return;
      }
    } while (!count.compareAndSet(n, n + REVOKED));
  }

  /** For buffers that were never handed out: revoke them and report them returned. */
  void abandon() {
    revoke();
    giveBack();
  }

  @Override
  public boolean release() {
    return release(1);
  }

  @Override
  public boolean release(int decrement) {
    if (decrement < 1) {
      throw new IllegalArgumentException("decrement must be >= 1");
    }
    int n = count.addAndGet(-decrement);
    int left = isRevoked(n) ? n - REVOKED : n;
    if (left < 0) {
      throw new IllegalStateException("Reference count went negative: " + left);
    }
    if (left == 0) {
      giveBack();
    }
    return left == 0;
  }

  @Override
  public void retain() {
    retain(1);
  }

  @Override
  public void retain(int increment) {
    if (increment < 1) {
      throw new IllegalArgumentException("increment must be >= 1");
    }
    int before = count.getAndAdd(increment);
    if (isRevoked(before)) {
      count.addAndGet(-increment);
      throw new IllegalStateException(ENDED);
    }
    if (before < 1) {
      count.addAndGet(-increment);
      throw new IllegalStateException("Cannot retain a buffer that was released");
    }
  }

  @Override
  public ArrowBuf retain(ArrowBuf source, BufferAllocator target) {
    retain();
    ArrowBuf retained = deriveBuffer(source, 0, source.capacity());
    retained.readerIndex(source.readerIndex());
    retained.writerIndex(source.writerIndex());
    return retained;
  }

  /** A slice shares the count of the buffer it was cut from, as Arrow's own slices do. */
  @Override
  public ArrowBuf deriveBuffer(ArrowBuf source, long index, long length) {
    return new ArrowBuf(this, null, length, source.memoryAddress() + index);
  }

  /**
   * No allocator can own the client's memory, so the "transferred" buffer is one more reference to
   * it. The caller releases the source, as it would after a real transfer.
   */
  @Override
  public OwnershipTransferResult transferOwnership(ArrowBuf source, BufferAllocator target) {
    ArrowBuf transferred = retain(source, target);
    return new OwnershipTransferResult() {
      @Override
      public boolean getAllocationFit() {
        return true;
      }

      @Override
      public ArrowBuf getTransferredBuffer() {
        return transferred;
      }
    };
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public long getSize() {
    return size;
  }

  /** Nothing: the memory is not taken from any allocator. */
  @Override
  public long getAccountedSize() {
    return 0;
  }

  private void giveBack() {
    if (returned.compareAndSet(false, true) && onReturned != null) {
      onReturned.run();
    }
  }
}
