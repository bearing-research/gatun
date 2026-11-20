package org.gatun.server.observability;

import org.gatun.protocol.Action;

/**
 * Captures context for a single request, used for structured logging and diagnostics.
 *
 * <p>This record is immutable and captures all relevant information at request start time.
 */
public record RequestContext(
    long sessionId, long requestId, byte action, String target, long startNanos) {

  /** Create a new request context with current timestamp. */
  public static RequestContext create(long sessionId, long requestId, byte action, String target) {
    return new RequestContext(sessionId, requestId, action, target, System.nanoTime());
  }

  /** Get action name for logging. */
  public String actionName() {
    return actionName(action);
  }

  /**
   * The schema's name for an action. Taken from the generated table, so an action added to the
   * schema has its name in logs and metrics without being listed anywhere else.
   */
  public static String actionName(byte action) {
    return action >= 0 && action < Action.names.length
        ? Action.names[action]
        : "Unknown(" + action + ")";
  }

  /** Calculate elapsed time in microseconds. */
  public long elapsedMicros() {
    return (System.nanoTime() - startNanos) / 1000;
  }
}
