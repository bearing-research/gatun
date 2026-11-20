package org.gatun.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.netty.NettyAllocationManager;
import org.apache.arrow.memory.rounding.DefaultRoundingPolicy;
import org.apache.arrow.memory.rounding.RoundingPolicy;

/**
 * Keeps the large blocks of Arrow memory that the server frees, and hands them out again.
 *
 * <p>Arrow's Netty allocator pools what fits in one of its chunks (4 MiB). A larger buffer is new
 * memory from the operating system every time, and touching new memory costs several times what
 * copying into it does: of the 110 ms a 128 MiB {@code arrow_to_java} took, about 70 were page
 * faults. A block that was used before has its pages.
 *
 * <p>What is kept is bounded twice: by size ({@value #MAX_BYTES}; the blocks returned longest ago
 * go first), and by time ({@value #IDLE_MS}: a block nobody asked for in that long is given back to
 * the operating system), so that a server which once moved a large table does not hold its memory
 * for good.
 *
 * <p>Blocks come from where Arrow gets its large buffers, Netty's unpooled allocator, and so count
 * against the same limit as before. A block that is kept counts too, which is why an allocation
 * that fails empties the pool and is tried once more.
 *
 * <p>A block is handed out as its last user left it, like every buffer Netty pools: Arrow does not
 * promise zeroed memory, and code that reads a buffer before writing it was already wrong for
 * buffers of up to 4 MiB.
 */
final class ArrowBlockPool implements AllocationManager.Factory {
  private static final Logger LOG = Logger.getLogger(ArrowBlockPool.class.getName());

  /** Most bytes kept; 0 turns the pool off. The default is the size of a client's shared memory. */
  static final String MAX_BYTES = "gatun.arrow.pool.max_bytes";

  /** How long a block is kept without being asked for; 0 keeps it until the server exits. */
  static final String IDLE_MS = "gatun.arrow.pool.idle_ms";

  private static final long DEFAULT_IDLE_MS = 60_000;

  // Leak detection is for buffers that are dropped; these are held until they are released
  private static final UnpooledByteBufAllocator BLOCKS = new UnpooledByteBufAllocator(true, true);

  /** A block of memory, in use or kept. */
  private static final class Block {
    final ByteBuf memory;
    final long address;
    final long capacity;
    long returnedAt; // nanoTime; guarded by lock

    Block(ByteBuf memory) {
      this.memory = memory;
      this.address = memory.memoryAddress();
      this.capacity = memory.capacity();
    }
  }

  // Up to this size Netty pools the memory itself
  private final long nettyChunk;
  private final long maxBytes;
  private final long idleNanos;
  private final ScheduledThreadPoolExecutor timer;

  private final ReentrantLock lock = new ReentrantLock();
  // The kept blocks, by capacity (the one returned last first: its pages are the likeliest to be
  // in the caches) and by the time they were returned (the oldest first). Guarded by lock.
  private final TreeMap<Long, ArrayDeque<Block>> bySize = new TreeMap<>();
  private final ArrayDeque<Block> byAge = new ArrayDeque<>();
  private long keptBytes;
  private boolean trimScheduled;

  private final LongAdder reused = new LongAdder();
  private final LongAdder allocated = new LongAdder();

  ArrowBlockPool(long maxBytes, long idleMillis) {
    this.nettyChunk =
        (long) PooledByteBufAllocator.defaultPageSize() << PooledByteBufAllocator.defaultMaxOrder();
    this.maxBytes = Math.max(0, maxBytes);
    this.idleNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0, idleMillis));
    if (this.maxBytes > 0 && idleNanos > 0) {
      timer =
          new ScheduledThreadPoolExecutor(
              1,
              task -> {
                Thread thread = new Thread(task, "gatun-arrow-pool");
                thread.setDaemon(true);
                return thread;
              });
      // No thread while nothing is kept
      timer.setKeepAliveTime(10, TimeUnit.SECONDS);
      timer.allowCoreThreadTimeOut(true);
    } else {
      timer = null;
    }
  }

  /**
   * The pool as the server's system properties configure it.
   *
   * @param sharedMemoryBytes the size of a client's shared memory: no single transfer is larger,
   *     which makes it the size worth keeping by default
   */
  static ArrowBlockPool fromProperties(long sharedMemoryBytes) {
    return new ArrowBlockPool(
        property(MAX_BYTES, sharedMemoryBytes), property(IDLE_MS, DEFAULT_IDLE_MS));
  }

  // Long.getLong takes a value it cannot read ("64MB", "0 ") for no value at all, and the server
  // would run with a pool its user had turned off
  private static long property(String name, long otherwise) {
    String value = System.getProperty(name);
    if (value == null) {
      return otherwise;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "-D" + name + " takes a whole number, not '" + value + "'", e);
    }
  }

  /**
   * How the server's allocator rounds the sizes it is asked for.
   *
   * <p>Arrow rounds a request under 16 MiB up to a power of two, the sizes Netty's pool deals in.
   * Netty's pool ends at its chunk size, 4 MiB: above that the rounding buys nothing, and it would
   * count a 9 MiB table as 16 MiB against the pool's limit.
   */
  RoundingPolicy rounding() {
    return size ->
        size <= nettyChunk
            ? DefaultRoundingPolicy.DEFAULT_ROUNDING_POLICY.getRoundedSize(size)
            : size;
  }

  @Override
  public AllocationManager create(BufferAllocator accountingAllocator, long size) {
    if (size > nettyChunk && size <= Math.min(maxBytes, Integer.MAX_VALUE)) {
      Block block = take(size);
      if (block == null) {
        // Never more than the pool may keep, or the largest tables would never be kept
        long capacity = Math.min(Math.min(sizeClass(size), maxBytes), Integer.MAX_VALUE);
        block = allocate((int) capacity, (int) size);
      }
      if (block != null) {
        return new Pooled(accountingAllocator, block, size);
      }
    }
    try {
      return NettyAllocationManager.FACTORY.create(accountingAllocator, size);
    } catch (OutOfMemoryError | OutOfMemoryException e) {
      // Arrow reports memory that Netty refused as an exception of its own
      if (!drain()) {
        throw e;
      }
      return NettyAllocationManager.FACTORY.create(accountingAllocator, size);
    }
  }

  @Override
  public ArrowBuf empty() {
    return NettyAllocationManager.FACTORY.empty();
  }

  /**
   * The capacity of a new block: the size asked for, taken to the next eighth of a power of two, so
   * that a table a little larger than the last one can use its block. At most an eighth more than
   * was asked for, and memory that is never touched is never backed by pages.
   */
  static long sizeClass(long size) {
    long step = Long.highestOneBit(size) >>> 3;
    return step == 0 ? size : (size + step - 1) & -step;
  }

  /** A kept block of at least {@code size} bytes and not much more, or null. */
  private Block take(long size) {
    lock.lock();
    try {
      var fit = bySize.ceilingEntry(size);
      // A much larger block is better left for a request that needs it
      if (fit == null || fit.getKey() > size + size / 4) {
        return null;
      }
      Block block = fit.getValue().pollFirst();
      if (fit.getValue().isEmpty()) {
        bySize.remove(fit.getKey());
      }
      byAge.remove(block);
      keptBytes -= block.capacity;
      reused.increment();
      return block;
    } finally {
      lock.unlock();
    }
  }

  /**
   * New memory: {@code capacity} bytes, or the {@code size} asked for if there is no room for more.
   * Null where Netty has no address to give (no Unsafe).
   */
  private Block allocate(int capacity, int size) {
    ByteBuf memory = newMemory(capacity);
    if (memory == null && drain()) {
      memory = newMemory(capacity);
    }
    if (memory == null) {
      // Rounding up must not refuse a buffer that fits as it was asked for
      try {
        memory = BLOCKS.directBuffer(size, size);
      } catch (OutOfMemoryError e) {
        // What Arrow throws when Netty has no memory for a buffer
        throw new OutOfMemoryException("Failure allocating buffer.", e);
      }
    }
    if (!memory.hasMemoryAddress()) {
      memory.release();
      return null;
    }
    allocated.increment();
    return new Block(memory);
  }

  /** Memory from Netty, or null if its limit on direct memory does not allow that much. */
  private static ByteBuf newMemory(int bytes) {
    try {
      return BLOCKS.directBuffer(bytes, bytes);
    } catch (OutOfMemoryError e) {
      return null;
    }
  }

  /** Keep a block that is no longer in use. */
  private void give(Block block) {
    List<Block> evicted = new ArrayList<>();
    boolean schedule;
    lock.lock();
    try {
      block.returnedAt = System.nanoTime();
      bySize.computeIfAbsent(block.capacity, k -> new ArrayDeque<>()).addFirst(block);
      byAge.addLast(block);
      keptBytes += block.capacity;
      // No block is larger than maxBytes, so the one just returned stays
      while (keptBytes > maxBytes) {
        evicted.add(removeOldest());
      }
      schedule = timer != null && !trimScheduled;
      trimScheduled |= schedule;
    } finally {
      lock.unlock();
    }
    free(evicted);
    if (schedule) {
      scheduleTrim(idleNanos);
    }
  }

  /**
   * Run {@link #trim} after {@code delayNanos}. The caller has set {@code trimScheduled}.
   *
   * <p>Not under the lock, and never failing: this runs while Arrow releases a buffer, and a timer
   * that cannot start its thread now must not keep the next block from trying again.
   */
  private void scheduleTrim(long delayNanos) {
    try {
      timer.schedule(this::trim, delayNanos, TimeUnit.NANOSECONDS);
    } catch (RuntimeException | OutOfMemoryError e) {
      lock.lock();
      try {
        trimScheduled = false;
      } finally {
        lock.unlock();
      }
      LOG.log(Level.WARNING, "Could not schedule the release of idle Arrow blocks", e);
    }
  }

  /** Give back the blocks nobody asked for in {@code idleNanos}. Runs on the timer. */
  private void trim() {
    List<Block> evicted = new ArrayList<>();
    long wait = -1;
    lock.lock();
    try {
      long now = System.nanoTime();
      while (!byAge.isEmpty() && now - byAge.peekFirst().returnedAt >= idleNanos) {
        evicted.add(removeOldest());
      }
      if (byAge.isEmpty()) {
        trimScheduled = false;
      } else {
        wait = idleNanos - (now - byAge.peekFirst().returnedAt);
      }
    } finally {
      lock.unlock();
    }
    free(evicted);
    if (wait >= 0) {
      scheduleTrim(wait);
    }
  }

  /** Give back every kept block. Returns whether there was one. */
  private boolean drain() {
    List<Block> evicted = new ArrayList<>();
    lock.lock();
    try {
      while (!byAge.isEmpty()) {
        evicted.add(removeOldest());
      }
    } finally {
      lock.unlock();
    }
    free(evicted);
    return !evicted.isEmpty();
  }

  // The caller holds the lock
  private Block removeOldest() {
    Block block = byAge.pollFirst();
    ArrayDeque<Block> same = bySize.get(block.capacity);
    same.remove(block);
    if (same.isEmpty()) {
      bySize.remove(block.capacity);
    }
    keptBytes -= block.capacity;
    return block;
  }

  // Outside the lock: unmapping a large block takes a while
  private static void free(List<Block> blocks) {
    for (Block block : blocks) {
      block.memory.release();
    }
  }

  /** Lines for the server's metrics report. */
  String report() {
    long bytes;
    int blocks;
    lock.lock();
    try {
      bytes = keptBytes;
      blocks = byAge.size();
    } finally {
      lock.unlock();
    }
    return String.format(
        "  pool_max_bytes: %d\n"
            + "  pool_kept_bytes: %d\n"
            + "  pool_kept_blocks: %d\n"
            + "  pool_blocks_reused: %d\n"
            + "  pool_blocks_allocated: %d\n",
        maxBytes, bytes, blocks, reused.sum(), allocated.sum());
  }

  /** Arrow's handle on a block: released when the last buffer on it is. */
  private final class Pooled extends AllocationManager {
    private final Block block;
    private final long size;

    Pooled(BufferAllocator accountingAllocator, Block block, long size) {
      super(accountingAllocator);
      this.block = block;
      this.size = size;
    }

    /**
     * What was asked for, not what the block holds: the allocator gives back this many bytes when
     * the block is released, and it took the size of the request.
     */
    @Override
    public long getSize() {
      return size;
    }

    @Override
    protected long memoryAddress() {
      return block.address;
    }

    @Override
    protected void release0() {
      give(block);
    }
  }
}
