package org.gatun;

/**
 * Data does not fit in the shared memory zone it has to travel through.
 *
 * <p>The Python client maps this class name to {@code PayloadTooLargeError} and reads both sizes
 * out of the message, so the "N bytes exceeds M byte" wording is part of the protocol.
 */
public class PayloadTooLargeException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public PayloadTooLargeException(String what, long size, long limit) {
    super(String.format("%s too large: %d bytes exceeds %d byte limit", what, size, limit));
  }
}
