package org.gatun.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.gatun.protocol.Action;
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
 * <p>The reader decodes a callback's answer itself, before it reads the next frame, and counts the
 * answers it has decoded. An answer can name an object that Python releases the moment the answer
 * is on its way; the release must not be applied first. One that follows on this connection cannot
 * be, and one that arrives on the session's own channel says how many answers had been sent before
 * it and waits for that count ({@link #awaitAnswers}).
 *
 * <p>Callers never touch the socket. They are threads this server does not own (a Spark listener
 * bus, an executor's workers), and interrupting a thread that is blocked on a channel closes the
 * channel for everybody. They put frames on a queue that one writer thread empties, and take their
 * answers from a queue the reader thread fills; an interrupt then ends that caller's wait and
 * nothing else.
 */
final class CallbackChannel {
  private static final Logger LOG = Logger.getLogger(CallbackChannel.class.getName());

  // The largest frame, in either direction: what a command or a response may be on the session's
  // own channel, so both paths carry the same calls
  private final int maxFrameSize;

  // Put on a queue to wake whoever waits on it once the connection is gone
  private static final Object CLOSED = new Object();

  private final SocketChannel socket;
  private final String name;
  private final Consumer<Command> notices;
  private final Function<Command, Object> decodeAnswer;
  private final LongFunction<ByteBuffer> callEnded;
  private final AtomicLong callIds = new AtomicLong(1);
  private final Map<Long, BlockingQueue<Object>> calls = new ConcurrentHashMap<>();
  private final BlockingQueue<ByteBuffer> outbox = new LinkedBlockingQueue<>();
  private volatile boolean closed;

  // A lock with a condition: awaitAnswers waits with a time limit
  private final ReentrantLock answersLock = new ReentrantLock();
  private final Condition answersChanged = answersLock.newCondition();
  private long answersDecoded; // guarded by answersLock

  /**
   * @param notices handles a frame that belongs to no call
   * @param decodeAnswer turns a CallbackResponse into what {@link #await} returns for it; must not
   *     throw
   * @param callEnded builds the reply to a command that Python sent for a call nobody waits on any
   *     more
   */
  CallbackChannel(
      SocketChannel socket,
      String name,
      int maxFrameSize,
      Consumer<Command> notices,
      Function<Command, Object> decodeAnswer,
      LongFunction<ByteBuffer> callEnded) {
    this.socket = socket;
    this.name = name;
    this.maxFrameSize = maxFrameSize;
    this.notices = notices;
    this.decodeAnswer = decodeAnswer;
    this.callEnded = callEnded;
  }

  boolean isClosed() {
    return closed;
  }

  /** Start a call: frames Python sends with the returned ID are kept for {@link #await}. */
  long begin() {
    long callId = callIds.getAndIncrement();
    BlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
    calls.put(callId, inbox);
    if (closed) {
      // close() may have gone through the calls before this one was added
      inbox.add(CLOSED);
    }
    return callId;
  }

  /**
   * Forget a call. Python may not know yet that nobody waits on it (the caller was interrupted),
   * and its callback may still send commands for it: those, and any that already arrived, are
   * answered by {@code callEnded} instead of being left without a reply.
   */
  void end(long callId) {
    BlockingQueue<Object> inbox = calls.remove(callId);
    if (inbox == null) {
      return;
    }
    for (Object item; (item = inbox.poll()) != null; ) {
      if (item instanceof Command command && command.action() == Action.NestedCommand) {
        refuse(callId);
      }
    }
  }

  private void refuse(long callId) {
    if (!closed) {
      outbox.add(callEnded.apply(callId));
    }
  }

  /** Queue one frame for Python. The buffer must not be touched again by the caller. */
  void send(ByteBuffer frame) throws IOException {
    if (closed) {
      throw new IOException("The callback connection is closed");
    }
    outbox.add(frame);
  }

  /**
   * Wait for what Python sends next for a call: the decoded answer, or the {@link Command} of
   * anything else.
   */
  Object await(long callId) throws IOException, InterruptedException {
    BlockingQueue<Object> inbox = calls.get(callId);
    if (inbox == null) {
      throw new IllegalStateException("Call " + callId + " was not begun");
    }
    Object item = inbox.take();
    if (item == CLOSED) {
      inbox.add(CLOSED); // for a caller that waits again
      throw new IOException("The callback connection closed while Python was being called");
    }
    return item;
  }

  /**
   * Wait until the reader has decoded {@code count} answers, the connection has closed or the time
   * is up.
   *
   * @return false if the time ran out first
   */
  boolean awaitAnswers(long count, long timeoutMs) throws InterruptedException {
    long remaining = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    answersLock.lock();
    try {
      while (answersDecoded < count && !closed) {
        if (remaining <= 0) {
          return false;
        }
        remaining = answersChanged.awaitNanos(remaining);
      }
      return true;
    } finally {
      answersLock.unlock();
    }
  }

  private void answerDecoded() {
    answersLock.lock();
    try {
      answersDecoded++;
      answersChanged.signalAll();
    } finally {
      answersLock.unlock();
    }
  }

  /** Serve the connection on the calling thread until it ends, then close it. */
  void run() {
    // A platform thread, like the reader (see GatunServer.openCallbackChannel): every frame
    // to Python waits for this thread to wake
    // Started inside the try: if it cannot be (the process is at its limit of threads, an
    // OutOfMemoryError), the channel must still be closed, or its callers wait for frames
    // that nothing writes
    Thread writer = null;
    try {
      writer = Thread.ofPlatform().daemon(true).name(name + "-writer").start(this::writeLoop);
      readLoop();
    } catch (IOException | RuntimeException | OutOfMemoryError e) {
      LOG.log(closed ? Level.FINE : Level.WARNING, name + " ended: " + e);
    } finally {
      close();
      if (writer != null) {
        writer.interrupt();
      }
    }
  }

  private void readLoop() throws IOException {
    ByteBuffer lengthBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    while (readFully(lengthBuf)) {
      lengthBuf.flip();
      int size = lengthBuf.getInt();
      lengthBuf.clear();
      if (size <= 0 || size > maxFrameSize) {
        throw new IOException("Invalid callback frame size: " + size);
      }
      ByteBuffer frame = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
      if (!GatunServer.readSliced(socket, frame)) {
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
      boolean isAnswer = command.action() == Action.CallbackResponse;
      // Added under the map's lock for the key, so that end() either sees the frame in the
      // inbox it removes or has removed the inbox already
      BlockingQueue<Object> inbox =
          calls.computeIfPresent(
              callId,
              (id, waiting) -> {
                waiting.add(isAnswer ? decodeAnswer.apply(command) : command);
                return waiting;
              });
      if (inbox == null) {
        // The caller gave up (it was interrupted) before Python answered
        LOG.fine(name + ": no one waits for call " + callId);
        if (command.action() == Action.NestedCommand) {
          refuse(callId);
        }
      }
      if (isAnswer) {
        answerDecoded();
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
        GatunServer.writeSliced(socket, frame);
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
    for (BlockingQueue<Object> inbox : calls.values()) {
      inbox.add(CLOSED);
    }
    answersLock.lock();
    try {
      answersChanged.signalAll();
    } finally {
      answersLock.unlock();
    }
  }
}
