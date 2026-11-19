package org.gatun.server.observability;

/**
 * Per-session observability state.
 *
 * <p>Each client session gets its own observer instance that tracks:
 *
 * <ul>
 *   <li>Request history via ring buffer
 *   <li>Session-level counters
 *   <li>JFR event state
 * </ul>
 */
public class SessionObserver {
  private final long sessionId;
  private final RequestRingBuffer ringBuffer;
  private final long startTimeNanos;

  private long requestCount = 0;
  private RequestContext currentRequest = null;
  private GatunEvents.RequestEvent currentJfrEvent = null;

  public SessionObserver(long sessionId) {
    this.sessionId = sessionId;
    this.ringBuffer = new RequestRingBuffer();
    this.startTimeNanos = System.nanoTime();
  }

  public long getSessionId() {
    return sessionId;
  }

  public RequestRingBuffer getRingBuffer() {
    return ringBuffer;
  }

  /** Called when a request starts. */
  public void onRequestStart(long requestId, byte action, String target) {
    currentRequest = RequestContext.create(sessionId, requestId, action, target);

    // Start JFR event if enabled
    currentJfrEvent =
        GatunEvents.beginRequest(sessionId, requestId, currentRequest.actionName(), target);
  }

  /** Called when a request completes. */
  public void onRequestEnd(boolean success, String errorType, String errorMessage) {
    if (currentRequest == null) return;

    requestCount++;

    // Record in ring buffer
    ringBuffer.record(currentRequest, success, errorType, errorMessage);

    // Log structured output
    StructuredLogger.logRequest(currentRequest, success, errorType);

    // Update global metrics
    Metrics.getInstance()
        .recordRequest(currentRequest.action(), currentRequest.elapsedMicros(), success, errorType);

    // Complete JFR event
    if (currentJfrEvent != null) {
      GatunEvents.endRequest(currentJfrEvent, success, errorType);
      currentJfrEvent = null;
    }

    // Log error context if failed (not fatal - session continues)
    if (!success && errorMessage != null) {
      StructuredLogger.logError(
          sessionId,
          currentRequest.requestId(),
          currentRequest.actionName(),
          currentRequest.target(),
          errorType,
          errorMessage,
          ringBuffer,
          false /* isFatal */);
    }

    currentRequest = null;
  }

  /** Called when Arrow data is transferred. */
  public void onArrowTransfer(String direction, long rowCount, long bytesCopied, boolean zeroCopy) {
    recordArrowTransfer(sessionId, direction, rowCount, bytesCopied, zeroCopy);
  }

  /** As {@link #onArrowTransfer}, for a transfer made by a thread other than the session's own. */
  public static void recordArrowTransfer(
      long sessionId, String direction, long rowCount, long bytesCopied, boolean zeroCopy) {
    Metrics.getInstance().recordArrowTransfer(rowCount, bytesCopied);

    StructuredLogger.logArrowTransfer(sessionId, direction, rowCount, bytesCopied, zeroCopy);
    GatunEvents.emitArrowTransfer(sessionId, direction, rowCount, bytesCopied, zeroCopy);
  }

  /** Called when session starts. */
  public void onSessionStart(String shmPath) {
    Metrics.getInstance().updateSessionCount(1);

    StructuredLogger.logSessionStart(sessionId, shmPath);
    GatunEvents.emitSession(sessionId, "start", 0, 0);
  }

  /**
   * Called when session ends.
   *
   * @param objectsCreated objects registered for the session over its lifetime
   */
  public void onSessionEnd(long objectsCreated) {
    long durationMillis = (System.nanoTime() - startTimeNanos) / 1_000_000;
    Metrics.getInstance().updateSessionCount(-1);

    int objectCount = (int) Math.min(objectsCreated, Integer.MAX_VALUE);
    StructuredLogger.logSessionEnd(sessionId, objectCount, requestCount, durationMillis);
    GatunEvents.emitSession(sessionId, "end", objectCount, requestCount);
  }

  /** Get current request context (may be null if not in a request). */
  public RequestContext getCurrentRequest() {
    return currentRequest;
  }

  /** Get the number of requests processed. */
  public long getRequestCount() {
    return requestCount;
  }
}
