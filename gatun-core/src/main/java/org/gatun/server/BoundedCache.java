package org.gatun.server;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * A thread-safe bounded cache that evicts its oldest entries.
 *
 * <p>Designed for use in long-running processes like Spark drivers where unbounded caches can cause
 * memory issues with dynamically generated classes and methods.
 *
 * <p>Lookups take no lock: every method, constructor and field lookup of every session goes through
 * one of these, so a lock here would serialize all sessions' calls. That is why eviction is by age
 * (the order entries were added) rather than by last use, which would make every lookup a write. An
 * entry that is evicted while still in use is simply computed again.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class BoundedCache<K, V> {
  private final int maxSize;
  private final ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>();
  // Keys in the order they were added; the head is the next to be evicted
  private final ConcurrentLinkedQueue<K> order = new ConcurrentLinkedQueue<>();

  // Statistics for monitoring
  private final LongAdder hits = new LongAdder();
  private final LongAdder misses = new LongAdder();
  private final LongAdder evictions = new LongAdder();

  /**
   * Create a bounded cache with the specified maximum size.
   *
   * @param maxSize maximum number of entries before eviction (must be > 0)
   */
  public BoundedCache(int maxSize) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException("maxSize must be positive: " + maxSize);
    }
    this.maxSize = maxSize;
  }

  /**
   * Get a value from the cache.
   *
   * @param key the key to look up
   * @return the cached value, or null if not present
   */
  public V get(K key) {
    V value = map.get(key);
    if (value != null) {
      hits.increment();
    } else {
      misses.increment();
    }
    return value;
  }

  /**
   * Put a value into the cache.
   *
   * @param key the key
   * @param value the value
   */
  public void put(K key, V value) {
    if (map.put(key, value) == null) {
      added(key);
    }
  }

  /**
   * Get or compute a value if absent.
   *
   * <p>The mapping function runs without any lock held, so it may use this cache itself. Two
   * threads that miss the same key at once may both compute it; one result is kept.
   *
   * @param key the key
   * @param mappingFunction function to compute the value if absent
   * @return the existing or computed value
   */
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    V value = map.get(key);
    if (value != null) {
      hits.increment();
      return value;
    }
    misses.increment();
    value = mappingFunction.apply(key);
    if (value == null) {
      return null;
    }
    V existing = map.putIfAbsent(key, value);
    if (existing != null) {
      return existing;
    }
    added(key);
    return value;
  }

  /** Record a new key and evict the oldest entries until the cache is within its size. */
  private void added(K key) {
    order.add(key);
    while (map.size() > maxSize) {
      K oldest = order.poll();
      if (oldest == null) {
        break;
      }
      if (map.remove(oldest) != null) {
        evictions.increment();
      }
    }
  }

  /** Get the current size of the cache. */
  public int size() {
    return map.size();
  }

  /** Clear all entries from the cache. */
  public void clear() {
    map.clear();
    order.clear();
  }

  /** Get cache statistics as a formatted string. */
  public String getStats() {
    long hitCount = hits.sum();
    long total = hitCount + misses.sum();
    double hitRate = total > 0 ? (100.0 * hitCount / total) : 0.0;
    return String.format(
        "size=%d/%d hits=%d misses=%d evictions=%d hitRate=%.1f%%",
        map.size(), maxSize, hitCount, misses.sum(), evictions.sum(), hitRate);
  }

  /** Get the number of cache hits. */
  public long getHits() {
    return hits.sum();
  }

  /** Get the number of cache misses. */
  public long getMisses() {
    return misses.sum();
  }

  /** Get the number of evictions. */
  public long getEvictions() {
    return evictions.sum();
  }

  /** Get the maximum size of the cache. */
  public int getMaxSize() {
    return maxSize;
  }
}
