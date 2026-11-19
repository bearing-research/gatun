package org.gatun.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.gatun.protocol.Command;

/**
 * A session's callback connection: the socket on which threads other than the one running the
 * session's request call Python.
 *
 * <p>Frames are a four-byte little-endian length followed by a FlatBuffer. Any number of threads
 * have a call in flight at once; each call has an ID, and the reader hands every frame Python sends
 * to the call its {@code call_id} names. A frame with no call ID is not part of any call (Python
 * releasing objects it was passed) and goes to the handler the channel was made with, on the reader
 * thread.
 *
 * <p>Callers never touch the socket. They are threads this server does not own (a Spark listener
 * bus, an executor's workers), and interrupting a thread that is blocked on a channel closes the
 * channel for everybody. They put frames on a queue that one writer thread empties, and take their
 * answers from a queue the reader thread fills; an interrupt then ends that caller's wait and
 * nothing else.
 */
final class CallbackChannel {
  private static final Logger LOG = Logger.getLogger(CallbackChannel.class.getName());

  /** The size of the zones the in-band path uses, so both paths carry the same calls. */
  static final int MAX_FRAME_SIZE = 64 * 1024;

  // Put on a queue to wake whoever waits on it once the connection is gone
  private static final ByteBuffer CLOSED = ByteBuffer.allocate(0);

  private final SocketChannel socket;
  private final String name;
  private final Consumer<Command> notices;
  private final AtomicLong callIds = new AtomicLong(1);
  private final Map<Long, BlockingQueue<ByteBuffer>> calls = new ConcurrentHashMap<>();
  private final BlockingQueue<ByteBuffer> outbox = new LinkedBlockingQueue<>();
  private volatile boolean closed;

  CallbackChannel(SocketChannel socket, String name, Consumer<Command> notices) {
    this.socket = socket;
    this.name = name;
    this.notices = notices;
  }

  boolean isClosed() {
    return closed;
  }

  /** Start a call: frames Python sends with the returned ID are kept for {@link #await}. */
  long begin() {
    long callId = callIds.getAndIncrement();
    BlockingQueue<ByteBuffer> inbox = new LinkedBlockingQueue<>();
    calls.put(callId, inbox);
    if (closed) {
      // close() may have gone through the calls before this one was added
      inbox.add(CLOSED);
    }
    return callId;
  }

  /** Forget a call. Frames that still arrive for it are dropped. */
  void end(long callId) {
    calls.remove(callId);
  }

  /** Queue one frame for Python. The buffer must not be touched again by the caller. */
  void send(ByteBuffer frame) throws IOException {
    if (closed) {
      throw new IOException("The callback connection is closed");
    }
    outbox.add(frame);
  }

  /** Wait for the next frame of a call. */
  ByteBuffer await(long callId) throws IOException, InterruptedException {
    BlockingQueue<ByteBuffer> inbox = calls.get(callId);
    if (inbox == null) {
      throw new IllegalStateException("Call " + callId + " was not begun");
    }
    ByteBuffer frame = inbox.take();
    if (frame == CLOSED) {
      inbox.add(CLOSED); // for a caller that waits again
      throw new IOException("The callback connection closed while Python was being called");
    }
    return frame;
  }

  /** Serve the connection on the calling thread until it ends, then close it. */
  void run() {
    Thread writer = Thread.ofVirtual().name(name + "-writer").start(this::writeLoop);
    try {
      readLoop();
    } catch (IOException | RuntimeException e) {
      LOG.log(closed ? Level.FINE : Level.WARNING, name + " ended: " + e);
    } finally {
      close();
      writer.interrupt();
    }
  }

  private void readLoop() throws IOException {
    ByteBuffer lengthBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    while (readFully(lengthBuf)) {
      lengthBuf.flip();
      int size = lengthBuf.getInt();
      lengthBuf.clear();
      if (size <= 0 || size > MAX_FRAME_SIZE) {
        throw new IOException("Invalid callback frame size: " + size);
      }
      ByteBuffer frame = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
      if (!readFully(frame)) {
        return;
      }
      frame.flip();
      Command command = Command.getRootAsCommand(frame.duplicate().order(ByteOrder.LITTLE_ENDIAN));
      long callId = command.callId();
      if (callId == 0) {
        try {
          notices.accept(command);
        } catch (RuntimeException e) {
          LOG.log(Level.WARNING, name + ": a notice from Python failed", e);
        }
        continue;
      }
      BlockingQueue<ByteBuffer> inbox = calls.get(callId);
      if (inbox != null) {
        inbox.add(frame);
      } else {
        // The caller gave up (it was interrupted) before Python answered
        LOG.fine(name + ": dropped a frame for call " + callId);
      }
    }
  }

  private boolean readFully(ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      if (socket.read(buffer) == -1) {
        return false;
      }
    }
    return true;
  }

  private void writeLoop() {
    ByteBuffer lengthBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    try {
      while (!closed) {
        ByteBuffer frame = outbox.take();
        lengthBuf.clear();
        lengthBuf.putInt(frame.remaining()).flip();
        while (lengthBuf.hasRemaining()) {
          socket.write(lengthBuf);
        }
        while (frame.hasRemaining()) {
          socket.write(frame);
        }
      }
    } catch (InterruptedException e) {
      // run() is done with the connection
    } catch (IOException | RuntimeException e) {
      LOG.log(closed ? Level.FINE : Level.WARNING, name + " could not write: " + e);
      close();
    }
  }

  /** Close the socket and fail every call that is waiting. Safe to call more than once. */
  void close() {
    closed = true;
    try {
      socket.close();
    } catch (IOException e) {
      // already gone
    }
    for (BlockingQueue<ByteBuffer> inbox : calls.values()) {
      inbox.add(CLOSED);
    }
  }
}
