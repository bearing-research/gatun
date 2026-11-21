package org.gatun.server;

import com.google.flatbuffers.FlatBufferBuilder;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import jdk.net.ExtendedSocketOptions;
import jdk.net.UnixDomainPrincipal;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.gatun.PayloadTooLargeException;
import org.gatun.protocol.*; // Import all generated classes
import org.gatun.server.MethodResolver.MethodWithArgs;
import org.gatun.server.observability.GatunEvents;
import org.gatun.server.observability.Metrics;
import org.gatun.server.observability.RequestContext;
import org.gatun.server.observability.SessionObserver;
import org.gatun.server.observability.StructuredLogger;

public class GatunServer {
  private static final Logger LOG = Logger.getLogger(GatunServer.class.getName());

  // Protocol version - increment when making breaking changes to the protocol
  // Version 2: Per-client shared memory (handshake includes SHM path)
  // Version 4: Callback connection (Action.OpenCallbackChannel, call_id)
  public static final int PROTOCOL_VERSION = 6;

  private static final Set<PosixFilePermission> OWNER_ONLY =
      PosixFilePermissions.fromString("rw-------");

  private final Path socketPath;
  private volatile UserPrincipal serverUser; // owner of the socket file, set once it is bound
  private final long memorySize;
  private final long responseOffset;
  private final ExecutorService threadPool;

  // Memory zones
  private static final int COMMAND_OFFSET = 0;
  private static final int PAYLOAD_OFFSET =
      65536; // 64KB - increased to handle large pickled functions
  private static final int RESPONSE_ZONE_SIZE = 65536; // 64KB - increased to handle large responses

  // --- SECURITY: classes that can be instantiated or used for static methods ---
  private final ClassAllowlist allowlist = ClassAllowlist.fromSystemProperties();

  /** A static method or field: the class must be allowed and the member not refused. */
  private void requireClassAllowed(String className, String member) {
    requireClassAllowed(className);
    allowlist.checkStaticMember(className, member);
  }

  private void requireClassAllowed(String className) {
    if (!allowlist.isAllowed(className)) {
      throw new SecurityException(
          "Class not allowed: "
              + className
              + ". To allow it, pass allowed_classes or allowed_prefixes to connect(), or set"
              + " the gatun.allowlist.classes / gatun.allowlist.prefixes system properties.");
    }
  }

  // --- ARROW TABLES MADE FOR CLIENTS ---
  // Tables created by ArrowToObject hold off-heap memory that no garbage collector reclaims, so
  // the server closes each one when its last registry entry goes away. Tables that Java code
  // produced itself are not in here: whoever made them closes them.
  private final Map<Object, OwnedTable> ownedTables = new java.util.IdentityHashMap<>();

  private static final class OwnedTable {
    int references = 0; // registry entries pointing at the table; guarded by ownedTables
    final long bytes;
    final SessionQuota quota;

    OwnedTable(long bytes, SessionQuota quota) {
      this.bytes = bytes;
      this.quota = quota;
    }
  }

  // Tables a client has lent (ArrowForm.LentTable), by the ID each was registered under. Freeing
  // that ID is how the client takes the table back, whatever else refers to it by then.
  private final Map<Long, ArrowLoans.Loan> lentTables = new ConcurrentHashMap<>();

  // --- THE OBJECT REGISTRY ---
  private final Map<Long, Object> objectRegistry = new ConcurrentHashMap<>();
  private final AtomicLong objectIdCounter = new AtomicLong(1);

  // Session counter for unique per-client SHM paths
  private final AtomicLong sessionCounter = new AtomicLong(0);

  // Files of the sessions that are still open (shared memory, a callback listener's socket), so
  // that shutdown can remove them
  private final Set<Path> sessionFiles = ConcurrentHashMap.newKeySet();

  // --- CALLBACK REGISTRY ---
  // Maps callback ID -> CallbackInfo (interface name + session context). Not the proxy: held
  // here it would never be garbage collected, and a callback nothing can call would stay
  // Note: callback IDs are allocated from objectIdCounter to ensure uniqueness
  private final Map<Long, CallbackInfo> callbackRegistry = new ConcurrentHashMap<>();

  /**
   * Holds session context for callback invocations. This is captured by the callback proxy so that
   * callbacks can be invoked from any thread, not just the handleClient thread.
   */
  private static class SessionContext {
    final SocketChannel channel;
    final MemorySegment sharedMem;
    final Set<Long> sessionObjectIds; // Track object IDs for cleanup
    final Set<Long> sessionCallbackIds;
    final SessionQuota quota;
    final SessionControl control = new SessionControl();

    // The callback connection, for threads other than the session's own. The listener is set
    // from OpenCallbackChannel until the client has connected to it; the channel from then on.
    volatile ServerSocketChannel callbackListener;
    volatile CallbackChannel callbackChannel;
    // Set when the session ends. Its proxies can outlive it in whatever Java code holds them.
    volatile boolean closed;
    // Calls on the callback connection that Python has not answered yet. The session is in use
    // for as long as there are any, whatever its own channel is doing.
    final AtomicInteger channelCalls = new AtomicInteger();

    SessionContext(
        SocketChannel channel,
        MemorySegment sharedMem,
        Set<Long> sessionObjectIds,
        Set<Long> sessionCallbackIds,
        SessionQuota quota) {
      this.channel = channel;
      this.sharedMem = sharedMem;
      this.sessionObjectIds = sessionObjectIds;
      this.sessionCallbackIds = sessionCallbackIds;
      this.quota = quota;
    }
  }

  /**
   * Lets another connection cancel the request a session is running.
   *
   * <p>A session's own channel is busy for as long as its request runs, so a cancel has to come in
   * on a different connection, which names the session by its shared memory path. Cancelling
   * interrupts the session's thread, but only while it is running the request's Java code:
   * interrupting a thread that is reading or writing the session's socket would close the socket.
   */
  private static final class SessionControl {
    private final Thread thread = Thread.currentThread();

    private boolean requestActive;
    private boolean interruptible;
    private boolean cancelled;

    synchronized void beginRequest() {
      requestActive = true;
      interruptible = true;
      cancelled = false;
    }

    /** The request's Java code is done; from here on the thread uses the socket again. */
    synchronized void endRequest() {
      requestActive = false;
      interruptible = false;
      cancelled = false;
      Thread.interrupted(); // drop an interrupt that arrived too late to matter
    }

    /** Called by another session's thread. False if there was no request to cancel. */
    synchronized boolean cancel() {
      if (!requestActive) {
        return false;
      }
      cancelled = true;
      if (interruptible) {
        thread.interrupt();
      }
      return true;
    }

    /** Before a callback round trip, which uses the socket. */
    synchronized void beginSocketUse() throws InterruptedException {
      interruptible = false;
      if (Thread.interrupted() || cancelled) {
        throw new InterruptedException("Request was cancelled");
      }
    }

    /** After a callback round trip; the caller checks for a cancel that arrived during it. */
    synchronized void endSocketUse() {
      interruptible = requestActive;
    }

    synchronized boolean isCancelled() {
      return cancelled;
    }

    synchronized boolean isRequestActive() {
      return requestActive;
    }

    boolean isSessionThread() {
      return Thread.currentThread() == thread;
    }
  }

  // Sessions by shared memory path, for Cancel and for reaping idle sessions
  private final Map<String, SessionContext> sessions = new ConcurrentHashMap<>();
  private final AtomicInteger sessionCount = new AtomicInteger();

  // Per-session context stored in ThreadLocal for the handleClient thread
  private static final ThreadLocal<SessionContext> sessionContext = new ThreadLocal<>();

  // Per-session: current request ID for cancellation support
  private static final ThreadLocal<Long> currentRequestId = new ThreadLocal<>();

  // Per-request: nesting depth counter for convertArgument recursion
  private static final ThreadLocal<Integer> argumentNestingDepth = ThreadLocal.withInitial(() -> 0);

  // --- REUSABLE FLATBUFFER TEMP OBJECTS ---
  // These are ThreadLocal to avoid per-request allocation overhead.
  // FlatBuffer table wrappers are lightweight and can be reused via Init().
  // Only leaf value types are shared this way. Container types (ListVal, MapVal, ArrayVal) must
  // NOT be: convertArgument recurses into their elements, and a shared wrapper would be
  // re-pointed at the inner collection while the outer loop is still reading from it.
  private static final ThreadLocal<StringVal> tempStringVal =
      ThreadLocal.withInitial(StringVal::new);
  private static final ThreadLocal<IntVal> tempIntVal = ThreadLocal.withInitial(IntVal::new);
  private static final ThreadLocal<DoubleVal> tempDoubleVal =
      ThreadLocal.withInitial(DoubleVal::new);
  private static final ThreadLocal<BoolVal> tempBoolVal = ThreadLocal.withInitial(BoolVal::new);
  private static final ThreadLocal<CharVal> tempCharVal = ThreadLocal.withInitial(CharVal::new);
  private static final ThreadLocal<ObjectRef> tempObjectRef =
      ThreadLocal.withInitial(ObjectRef::new);
  private static final ThreadLocal<CallbackRef> tempCallbackRef =
      ThreadLocal.withInitial(CallbackRef::new);

  // The IDs of the objects registered by the pack running on this thread, when its caller wants
  // them: to count them against the quota, and to free them if the client never gets the result.
  // Counting the session's objects before and after, or taking the range of IDs handed out,
  // would also pick up what other threads registered for the same session meanwhile.
  private static final ThreadLocal<List<Long>> packRecorder = new ThreadLocal<>();

  private final ArrowBlockPool arrowPool;
  private final BufferAllocator allocator;

  // Delegate to ReflectionCache for all caching operations
  private static java.lang.reflect.Constructor<?> getNoArgConstructor(Class<?> clazz)
      throws NoSuchMethodException {
    return ReflectionCache.getNoArgConstructor(clazz);
  }

  private static MethodHandle getNoArgMethodHandle(Class<?> clazz, String methodName) {
    return ReflectionCache.getNoArgMethodHandle(clazz, methodName);
  }

  private static java.lang.reflect.Field getStaticField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    return ReflectionCache.getStaticField(clazz, fieldName);
  }

  private static java.lang.reflect.Field getInstanceField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    return ReflectionCache.getInstanceField(clazz, fieldName);
  }

  private static java.lang.invoke.VarHandle getInstanceVarHandle(Class<?> clazz, String fieldName) {
    return ReflectionCache.getInstanceVarHandle(clazz, fieldName);
  }

  private static ReflectionCache.CachedStaticVarHandle getStaticVarHandle(
      Class<?> clazz, String fieldName) {
    return ReflectionCache.getStaticVarHandle(clazz, fieldName);
  }

  private static java.lang.reflect.Method[] getCachedMethods(Class<?> clazz) {
    return ReflectionCache.getCachedMethods(clazz);
  }

  private static Class<?> getClass(String className) throws ClassNotFoundException {
    return ReflectionCache.getClass(className);
  }

  private static Class<?> tryGetClass(String className) {
    return ReflectionCache.tryGetClass(className);
  }

  /** Holds information about a registered callback. */
  private static class CallbackInfo {
    final long id;
    final String interfaceName;
    final SessionContext sessionContext; // Captured for cross-thread callback invocations

    // Not the proxy: an entry here must not be what keeps it alive (see proxyCollected)
    CallbackInfo(long id, String interfaceName, SessionContext ctx) {
      this.id = id;
      this.interfaceName = interfaceName;
      this.sessionContext = ctx;
    }
  }

  // Tells when a callback's proxy has been garbage collected
  private static final Cleaner PROXY_CLEANER = Cleaner.create();

  public GatunServer(String socketPathStr, long memorySize) {
    this.socketPath = Path.of(socketPathStr);
    this.memorySize = memorySize;
    this.responseOffset = memorySize - RESPONSE_ZONE_SIZE;
    // A session's thread spends its life blocked on its socket, and a platform thread wakes
    // from that in one step: the kernel makes it runnable. A virtual thread needs the poller
    // thread to notice the socket and then a carrier to be woken for it, which made every
    // request about 30 microseconds slower. A program has a handful of sessions, so a thread
    // each costs little, and a connection whose thread cannot be started is refused (see
    // start()). A new thread per session, never a reused one: the session's thread-locals end
    // with it.
    this.threadPool =
        Executors.newThreadPerTaskExecutor(
            Thread.ofPlatform().daemon(true).name("gatun-session-", 1).factory());
    // Large Arrow buffers come from a pool of the server's own (see ArrowBlockPool)
    this.arrowPool = ArrowBlockPool.fromProperties(memorySize);
    this.allocator =
        new RootAllocator(
            RootAllocator.configBuilder()
                .allocationManagerFactory(arrowPool)
                .roundingPolicy(arrowPool.rounding())
                .build());
    Metrics.getInstance().setArrowPoolReport(arrowPool::report);
  }

  public void start() throws IOException {
    Files.deleteIfExists(socketPath);

    // --- Register shutdown hook for cleanup ---
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(() -> {
              LOG.info("Shutting down...");
              // Remove the files first: the JVM may exit before the session threads get to
              // their own cleanup, and closing the allocator can throw.
              deleteQuietly(socketPath);
              for (Path shmPath : sessionFiles) {
                deleteQuietly(shmPath);
              }
              threadPool.shutdownNow();
              allocator.close();
            }));

    // --- START SOCKET SERVER ---
    try (ServerSocketChannel serverChannel =
        ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
      serverChannel.bind(UnixDomainSocketAddress.of(socketPath));
      // Whoever can connect can run code as this user, so only this user may
      Files.setPosixFilePermissions(socketPath, OWNER_ONLY);
      serverUser = Files.getOwner(socketPath);
      LOG.info("Server ready at: " + socketPath);
      if (allowlist.hasAdditions()) {
        LOG.info("Class allowlist additions: " + allowlist.describeAdditions());
      }

      if (ResourceLimits.SESSION_IDLE_TIMEOUT_MS > 0) {
        Thread.ofVirtual().name("gatun-idle-reaper").start(this::reapIdleSessions);
      }

      while (true) {
        SocketChannel client;
        try {
          client = serverChannel.accept();
        } catch (java.nio.channels.ClosedChannelException e) {
          throw e; // the server socket is gone; nothing more to accept
        } catch (IOException e) {
          // Typically out of file descriptors. That must not end the sessions already
          // running; wait for some to close.
          LOG.log(Level.WARNING, "Could not accept a connection", e);
          try {
            Thread.sleep(100);
          } catch (InterruptedException interrupted) {
            return;
          }
          continue;
        }
        if (!isSameUser(client)) {
          LOG.warning("Refused a connection from another user");
          client.close();
          continue;
        }
        long sessionId = sessionCounter.incrementAndGet();
        try {
          threadPool.submit(() -> {
            if (!takeSessionSlot()) {
              LOG.warning(
                  "Refused a connection: "
                      + ResourceLimits.MAX_SESSIONS
                      + " sessions are open (max_sessions)");
              try {
                client.close();
              } catch (IOException e) {
                // already gone
              }
              return;
            }
            handleClient(client, sessionId);
          });
        } catch (OutOfMemoryError | RuntimeException e) {
          // No thread for it: the process is at its limit of threads, which the JVM reports
          // as an OutOfMemoryError. Like running out of file descriptors above, that refuses
          // this connection and must not end the sessions already running.
          LOG.log(Level.WARNING, "Refused a connection: could not start its thread", e);
          try {
            client.close();
          } catch (IOException closing) {
            // already gone
          }
        }
      }
    }
  }

  /**
   * Create a session's shared memory file: new, readable and writable by this user only, and {@code
   * memorySize} long.
   *
   * <p>Everything a client sends and receives passes through this file, so nobody else may read it,
   * and it must not be a file or link that someone else put at this predictable path. A stale file
   * from an earlier server is removed first; if that or the exclusive create fails, the session is
   * refused.
   *
   * <p>The file is extended by writing its last byte, which leaves the rest as a hole: pages read
   * as zero and take up space only once touched. Writing zeros over the whole file made every
   * connection pay for its full size.
   */
  private FileChannel createSharedMemoryFile(Path path) throws IOException {
    return createSharedMemoryFile(path, callMemoryBytes.get(), "the session's shared memory");
  }

  /**
   * @param promised the bytes of free space that other files of this kind may still come to use,
   *     which this one must leave them
   * @param purpose what the file is for, as the error for a full disk names it
   */
  private FileChannel createSharedMemoryFile(Path path, long promised, String purpose)
      throws IOException {
    // The file is sparse, so a full disk would not show until a page is first written, and
    // then as a fault that takes the whole JVM down. Refuse the session instead.
    long usable = Files.getFileStore(path.toAbsolutePath().getParent()).getUsableSpace();
    if (usable - promised < this.memorySize) {
      throw new IOException(
          "Not enough free space for "
              + purpose
              + ": "
              + this.memorySize
              + " bytes needed, "
              + usable
              + " available in "
              + path.toAbsolutePath().getParent()
              + (promised > 0
                  ? ", " + promised + " of them set aside for tables that callbacks are moving"
                  : ""));
    }
    Files.deleteIfExists(path); // removes a link itself, never what it points at
    FileChannel channel =
        FileChannel.open(
            path,
            Set.of(
                StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE),
            PosixFilePermissions.asFileAttribute(OWNER_ONLY));
    try {
      channel.write(ByteBuffer.allocate(1), this.memorySize - 1);
    } catch (IOException | RuntimeException e) {
      channel.close();
      throw e;
    }
    return channel;
  }

  // How long a connection waits for a session slot before it is refused
  private static final long SESSION_SLOT_WAIT_MS = 2_000;

  /**
   * Count one more session, if the limit allows. A slot is given back when the server notices its
   * session's socket closing, a moment after the client closed it, so a connection that arrives
   * just then (a client connecting right after the launcher's check that the server is up) waits
   * briefly instead of being refused.
   */
  private boolean takeSessionSlot() {
    int limit = ResourceLimits.MAX_SESSIONS;
    long deadline = System.nanoTime() + SESSION_SLOT_WAIT_MS * 1_000_000;
    while (true) {
      int open = sessionCount.get();
      if (limit <= 0 || open < limit) {
        if (sessionCount.compareAndSet(open, open + 1)) {
          return true;
        }
        continue;
      }
      if (System.nanoTime() >= deadline) {
        return false;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        return false;
      }
    }
  }

  /**
   * Whether the process at the other end of a connection runs as the server's user. The socket
   * file's permissions already keep other users out; this also covers the moment between creating
   * the socket and restricting it, and a socket directory with looser rules.
   */
  private boolean isSameUser(SocketChannel client) {
    if (serverUser == null) {
      return true;
    }
    try {
      UnixDomainPrincipal peer = client.getOption(ExtendedSocketOptions.SO_PEERCRED);
      return serverUser.equals(peer.user());
    } catch (UnsupportedOperationException | IOException e) {
      return true; // the platform cannot tell us; the file permissions still apply
    }
  }

  /**
   * Close sessions that have been idle past the timeout. Their threads are blocked reading the
   * socket, so without this an idle session would only be noticed when its next request came.
   */
  private void reapIdleSessions() {
    long interval = Math.max(50, Math.min(ResourceLimits.SESSION_IDLE_TIMEOUT_MS / 2, 5_000));
    while (true) {
      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        return;
      }
      for (SessionContext session : sessions.values()) {
        if (session.quota.isTimedOut()
            && !session.control.isRequestActive()
            && session.channelCalls.get() == 0) {
          LOG.warning("Closing idle session " + session.quota.getSessionId());
          try {
            session.channel.close();
          } catch (IOException e) {
            // already closing
          }
        }
      }
    }
  }

  private static void deleteQuietly(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      // Nothing useful to do about a cleanup failure during shutdown
    }
  }

  /**
   * Start listening for the session's callback connection and return the socket's path.
   *
   * <p>The connection is asked for on the session's own channel and made to a socket that exists
   * for this one session and accepts one connection, so it needs no handshake of its own to say
   * whose it is. It takes no session slot and has no shared memory.
   */
  private Path openCallbackListener(SessionContext ctx) throws IOException {
    if (ctx.callbackListener != null || ctx.callbackChannel != null) {
      throw new IllegalStateException("The session's callback connection was already opened");
    }
    long sessionId = ctx.quota.getSessionId();
    Path path = Path.of(socketPath.toString() + "." + sessionId + ".cb");
    Files.deleteIfExists(path);
    ServerSocketChannel listener = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
    try {
      listener.bind(UnixDomainSocketAddress.of(path));
      Files.setPosixFilePermissions(path, OWNER_ONLY);
    } catch (IOException | RuntimeException e) {
      listener.close();
      deleteQuietly(path);
      throw e;
    }
    sessionFiles.add(path);
    ctx.callbackListener = listener;
    // Platform threads for the connection's reader (this one) and its writer, as for the
    // session: each is blocked on a socket or a queue between frames and every callback
    // waits for both to wake
    try {
      Thread.ofPlatform()
          .daemon(true)
          .name("gatun-callbacks-" + sessionId)
          .start(() -> serveCallbackConnection(ctx, listener, path));
    } catch (OutOfMemoryError e) {
      // The process is at its limit of threads. The session goes on without the connection.
      ctx.callbackListener = null;
      listener.close();
      deleteQuietly(path);
      sessionFiles.remove(path);
      throw new IOException("Could not start a thread for the callback connection", e);
    }
    return path;
  }

  /** Accept the session's callback connection and serve it until it or the session ends. */
  private void serveCallbackConnection(
      SessionContext ctx, ServerSocketChannel listener, Path path) {
    String name = "Callback connection of session " + ctx.quota.getSessionId();
    SocketChannel peer = null;
    try {
      try {
        while (peer == null) {
          SocketChannel candidate = listener.accept();
          if (isSameUser(candidate)) {
            peer = candidate;
          } else {
            LOG.warning(name + ": refused a connection from another user");
            candidate.close();
          }
        }
      } finally {
        // One connection is all this socket is for
        listener.close();
        ctx.callbackListener = null;
        deleteQuietly(path);
        sessionFiles.remove(path);
      }

      CallbackChannel channel =
          new CallbackChannel(
              peer,
              name,
              notice -> handleChannelNotice(ctx, notice),
              answer -> decodeChannelAnswer(ctx, answer),
              this::callEndedReply);
      ctx.callbackChannel = channel;
      if (ctx.closed) {
        // The session ended while the client was connecting, and found no channel to close
        channel.close();
        return;
      }

      // Tells the client that calls will be served from here on
      ByteBuffer ready = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
      ready.putInt(PROTOCOL_VERSION).flip();
      while (ready.hasRemaining()) {
        peer.write(ready);
      }
      channel.run();
    } catch (IOException e) {
      // The session ended before the client connected, or the client went away
      LOG.fine(name + " ended: " + e);
      if (peer != null) {
        try {
          peer.close();
        } catch (IOException closing) {
          // already gone
        }
      }
    }
  }

  /**
   * A frame on the callback connection that belongs to no call: Python releasing objects. A client
   * that only receives callbacks, or sits in one long request, sends nothing on its own channel
   * that the release could ride on, and the arguments of every call would pile up.
   */
  private void handleChannelNotice(SessionContext ctx, Command notice) {
    if (notice.action() != Action.FreeObject) {
      throw new IllegalArgumentException("Unexpected action " + notice.action());
    }
    checkArgumentCount(notice.argsLength());
    // For the session ID in what the registry logs; this is the connection's reader thread
    sessionContext.set(ctx);
    try {
      freeObjects(notice, ctx.sessionObjectIds, ctx.quota);
    } finally {
      sessionContext.remove();
    }
  }

  /** The error type that tells the client Java no longer waits for a call. */
  private static final String CALL_ENDED = "org.gatun.server.CallbackCallEnded";

  /** The reply to a command that a callback sent for a call nobody waits on any more. */
  private ByteBuffer callEndedReply(long callId) {
    FlatBufferBuilder builder = new FlatBufferBuilder(128);
    builder.finish(
        packError(builder, "Java is no longer waiting for this callback", CALL_ENDED, callId));
    return builder.dataBuffer();
  }

  /**
   * Unregister the callback that {@code id} names: the ID {@code RegisterCallback} returned, or the
   * ID of any other handle the client has on the proxy. An object gets a new ID each time it is
   * returned to the client, so a proxy the client read back out of a Java collection is known to it
   * under another one.
   *
   * @return the ID of the callback that was unregistered, or 0 if there was none
   */
  private long unregisterCallback(long id, Set<Long> sessionCallbackIds) {
    long callbackId = id;
    if (!sessionCallbackIds.contains(id)) {
      Object object = sessionObject(id);
      if (object != null
          && Proxy.isProxyClass(object.getClass())
          && Proxy.getInvocationHandler(object) instanceof CallbackHandler handler) {
        callbackId = handler.callbackId();
      }
    }
    if (!sessionCallbackIds.remove(callbackId)) {
      return 0;
    }
    callbackRegistry.remove(callbackId);
    LOG.fine("Unregistered callback ID " + callbackId);
    return callbackId;
  }

  /** A callback proxy's handler, which says which callback the proxy is. */
  private record CallbackHandler(long callbackId, InvocationHandler calls)
      implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      return calls.invoke(proxy, method, args);
    }
  }

  /** What Python answered on the callback connection, decoded by the connection's reader. */
  private record CallbackAnswer(Object value, RuntimeException failure) {}

  /**
   * Decode a callback's answer on the callback connection's reader thread, which is to say before
   * anything that follows it on the connection is looked at. The answer can name objects that
   * Python releases as soon as it has sent the answer; decoded later, by the thread that waits for
   * it, they could be gone.
   */
  private CallbackAnswer decodeChannelAnswer(SessionContext ctx, Command answer) {
    sessionContext.set(ctx);
    try {
      return new CallbackAnswer(readCallbackResult(answer), null);
    } catch (RuntimeException e) {
      return new CallbackAnswer(null, e);
    } finally {
      sessionContext.remove();
    }
  }

  /**
   * Before releasing objects for a command that came on the session's own channel: wait until the
   * callback answers the client had sent by then have been decoded. See {@link CallbackChannel}.
   */
  private void awaitAnswersBefore(Command free) throws InterruptedException {
    long answersSent = free.answersSent();
    SessionContext ctx = sessionContext.get();
    CallbackChannel channel = ctx == null ? null : ctx.callbackChannel;
    if (answersSent <= 0 || channel == null) {
      return;
    }
    // The answers are on a local socket a thread is reading; the limit is for a client that
    // counted one it never managed to send
    if (!channel.awaitAnswers(answersSent, 5_000)) {
      LOG.warning("Releasing objects without having seen callback answer " + answersSent);
    }
  }

  /** End a session's callback connection; threads waiting for Python on it fail. */
  private void closeCallbackConnection(SessionContext ctx) {
    if (ctx == null) {
      return;
    }
    ctx.closed = true;
    ServerSocketChannel listener = ctx.callbackListener;
    if (listener != null) {
      try {
        listener.close();
      } catch (IOException e) {
        // already closed
      }
    }
    CallbackChannel channel = ctx.callbackChannel;
    if (channel != null) {
      channel.close();
    }
  }

  private void handleClient(SocketChannel client, long sessionId) {
    // Threads calling Python on the callback connection register the objects they pass
    Set<Long> sessionObjectIds = ConcurrentHashMap.newKeySet();
    // Concurrent: a callback on another thread can unregister one
    Set<Long> sessionCallbackIds = ConcurrentHashMap.newKeySet();
    SessionObserver observer = new SessionObserver(sessionId);
    SessionQuota quota = new SessionQuota(sessionId);
    LOG.fine("New client session " + sessionId + " started");

    // Create per-session shared memory file
    Path sessionShmPath = Path.of(socketPath.toString() + "." + sessionId + ".shm");
    sessionFiles.add(sessionShmPath);

    // Tables the client lends are read through a mapping of their own, which is made by the
    // first loan and can outlive the session (see ArrowLoans)
    ArrowLoans loans = new ArrowLoans(this.memorySize, PAYLOAD_OFFSET, allocator, quota);

    // The Arrow handler is declared last so that it is closed first: the batch it holds points
    // into the mapping that the arena unmaps. The connection is not among these: it is closed
    // in the finally block, after the catch blocks have logged why the session ended.
    try (FileChannel channel = createSharedMemoryFile(sessionShmPath);
        Arena arena = Arena.ofConfined();
        ArrowMemoryHandler arrowHandler = new ArrowMemoryHandler(allocator)) {

      MemorySegment sharedMem =
          channel.map(FileChannel.MapMode.READ_WRITE, 0, this.memorySize, arena);
      loans.file(channel);

      LOG.fine("Session " + sessionId + " SHM mapped at: " + sessionShmPath);

      // Set up session context for callbacks (captured by callback proxies)
      SessionContext ctx =
          new SessionContext(client, sharedMem, sessionObjectIds, sessionCallbackIds, quota);
      sessionContext.set(ctx);
      SessionControl control = ctx.control;
      sessions.put(sessionShmPath.toString(), ctx);
      // Each command is copied here before it is read (see the request loop)
      byte[] commandCopy = new byte[(int) PAYLOAD_OFFSET];

      // --- HANDSHAKE: Send Protocol Version, Memory Size, and SHM Path to Client ---
      // Format: [4 bytes: version] [4 bytes: arena_epoch] [8 bytes: memory size]
      //         [2 bytes: shm_path_length] [N bytes: shm_path (UTF-8)]
      byte[] shmPathBytes =
          sessionShmPath.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      ByteBuffer handshakeBuf = ByteBuffer.allocate(16 + 2 + shmPathBytes.length);
      handshakeBuf.order(ByteOrder.LITTLE_ENDIAN);
      handshakeBuf.putInt(PROTOCOL_VERSION);
      handshakeBuf.putInt((int) arrowHandler.getArenaEpoch()); // Current epoch for synchronization
      handshakeBuf.putLong(this.memorySize);
      handshakeBuf.putShort((short) shmPathBytes.length);
      handshakeBuf.put(shmPathBytes);
      handshakeBuf.flip();

      while (handshakeBuf.hasRemaining()) {
        client.write(handshakeBuf);
      }

      // Notify observer of session start
      observer.onSessionStart(sessionShmPath.toString());

      ByteBuffer lengthBuf = ByteBuffer.allocate(4);
      lengthBuf.order(ByteOrder.LITTLE_ENDIAN);

      // Reusable FlatBufferBuilder for responses (avoid allocation per request)
      FlatBufferBuilder responseBuilder = new FlatBufferBuilder(1024);
      List<Long> packedIds = new ArrayList<>();

      while (readFully(client, lengthBuf)) {
        // A request that arrives after the idle timeout is answered with the reason before the
        // session closes; the reaper may not have got to this session yet
        if (quota.isTimedOut()) {
          LOG.warning("Session " + sessionId + " timed out due to inactivity");
          writeErrorResponse(
              client,
              sharedMem,
              "Session timed out after "
                  + ResourceLimits.SESSION_IDLE_TIMEOUT_MS
                  + " ms idle (gatun.limits.session_idle_timeout_ms). Reconnect to continue.",
              ResourceLimits.SessionTimeoutException.class.getName());
          break;
        }

        // Record request activity
        quota.recordRequest();

        // 1. Read Command Length
        lengthBuf.flip();
        int commandSize = lengthBuf.getInt();
        lengthBuf.clear();

        // Validate command size to prevent OOB access or DoS
        // Command zone is from COMMAND_OFFSET to PAYLOAD_OFFSET
        if (commandSize <= 0 || commandSize > PAYLOAD_OFFSET) {
          LOG.warning("Invalid command size: " + commandSize + " (max: " + PAYLOAD_OFFSET + ")");
          writeErrorResponse(
              client,
              sharedMem,
              "Invalid command size: " + commandSize + " bytes (max: " + PAYLOAD_OFFSET + ")",
              "java.lang.IllegalArgumentException");
          continue;
        }

        // The command is read from a private copy, not from shared memory. A callback
        // made while it runs puts Python's answer in the command zone, and a command that
        // is still being read then (the rest of a batch, the remaining calls of
        // InvokeMethods) would be read out of that answer. A copy also cannot be changed
        // by the client after it has been checked.
        MemorySegment.copy(
            sharedMem, ValueLayout.JAVA_BYTE, COMMAND_OFFSET, commandCopy, 0, commandSize);
        ByteBuffer cmdBuf = ByteBuffer.wrap(commandCopy, 0, commandSize);
        cmdBuf.order(ByteOrder.LITTLE_ENDIAN); // Essential for FlatBuffers

        // Define 'cmd' here so it is visible to the rest of the loop. Bytes that are not a
        // command fail here, before the request proper; the client still gets an answer.
        Command cmd;
        long requestId;
        try {
          cmd = Command.getRootAsCommand(cmdBuf);
          requestId = cmd.requestId();
          observer.onRequestStart(requestId, cmd.action(), cmd.targetName());
        } catch (RuntimeException e) {
          LOG.log(Level.WARNING, "Session " + sessionId + " sent a malformed command", e);
          writeErrorResponse(
              client, sharedMem, "Malformed command: " + e, "java.lang.IllegalArgumentException");
          continue;
        }

        // Cancel names another session by its shared memory path and asks for its running
        // request to be interrupted. It is answered at once: true if there was one.
        if (cmd.action() == Action.Cancel) {
          SessionContext target = sessions.get(String.valueOf(cmd.targetName()));
          boolean delivered = target != null && target != ctx && target.control.cancel();
          LOG.fine("Cancel for " + cmd.targetName() + (delivered ? " delivered" : " ignored"));
          observer.onRequestEnd(true, null, null);

          FlatBufferBuilder cancelBuilder = new FlatBufferBuilder(256);
          int cancelResponse = packSuccess(cancelBuilder, delivered);
          cancelBuilder.finish(cancelResponse);
          ByteBuffer cancelBuf = cancelBuilder.dataBuffer();
          int cancelSize = cancelBuf.remaining();

          MemorySegment cancelSlice = sharedMem.asSlice(this.responseOffset, cancelSize);
          cancelSlice.copyFrom(MemorySegment.ofBuffer(cancelBuf));

          lengthBuf.putInt(cancelSize);
          lengthBuf.flip();
          client.write(lengthBuf);
          lengthBuf.clear();
          continue;
        }

        currentRequestId.set(requestId);

        // --- Standard Request/Response Logic ---
        // Reuse the response builder (clear resets position but keeps buffer)
        responseBuilder.clear();
        FlatBufferBuilder builder = responseBuilder;
        int responseOffset = 0;
        Object result = null;
        String errorType = null;
        String errorMessage = null;
        boolean requestSuccess = true;
        // The objects registered while packing the result (for a batch, its sub-results)
        packedIds.clear();
        // Results of a vectorized action, which registers objects as it goes
        java.util.List<Object> vectorResults = null;
        // Set by the actions that build their own response instead of returning a result
        boolean responsePacked = false;

        control.beginRequest();
        try {
          checkArgumentCount(cmd.argsLength());
          if (cmd.action() == Action.FreeObject || cmd.freeIdsLength() > 0) {
            awaitAnswersBefore(cmd);
          }
          if (cmd.action() != Action.FreeObject) {
            // Handles the client dropped since its last command, released before this one runs
            freeRiding(cmd, sessionObjectIds, quota);
          }

          if (cmd.action() == Action.CreateObject) {
            String className = cmd.targetName();
            requireClassAllowed(className);

            // Refuse before running the constructor; the slot is taken once it succeeds
            quota.checkObjectLimit();

            Class<?> clazz = getClass(className);

            // Convert constructor arguments if provided
            int argCount = cmd.argsLength();
            Object instance;

            if (argCount == 0) {
              // No-arg constructor (cached)
              instance = getNoArgConstructor(clazz).newInstance();
            } else {
              // Constructor with arguments
              Object[] javaArgs = new Object[argCount];
              Class<?>[] argTypes = new Class<?>[argCount];

              for (int i = 0; i < argCount; i++) {
                Argument arg = cmd.args(i);
                Object[] converted = convertArgument(arg);
                javaArgs[i] = converted[0];
                argTypes[i] = (Class<?>) converted[1];
              }

              var ctorResult = MethodResolver.findConstructorWithArgs(clazz, argTypes, javaArgs);
              instance = ctorResult.cached.newInstance(ctorResult.args);
            }

            quota.allocateObject();
            long newId = objectIdCounter.getAndIncrement();
            putObject(newId, instance);
            sessionObjectIds.add(newId);
            result = new ObjectRefT(newId);
            LOG.fine("Created object ID " + newId + " of type " + className);

          } else if (cmd.action() == Action.FreeObject) {
            freeObjects(cmd, sessionObjectIds, quota);
            result = null;
          } else if (cmd.action() == Action.InvokeMethod) {
            long targetId = cmd.targetId();
            Object target = sessionObject(targetId);
            String methodName = cmd.targetName();

            if (target == null) throw new RuntimeException("Object " + targetId + " not found");
            allowlist.checkInstanceMember(target, methodName);

            int argCount = cmd.argsLength();

            // Fast path for no-arg methods (most common case: size(), isEmpty(), toString(), etc)
            // Uses MethodHandle for faster invocation after JIT warmup
            if (argCount == 0) {
              MethodHandle handle = getNoArgMethodHandle(target.getClass(), methodName);
              if (handle != null) {
                result = handle.invoke(target);
              } else {
                // Fallback: use normal resolution (handles inherited methods, etc)
                MethodWithArgs mwa =
                    MethodResolver.findMethodWithArgs(
                        target.getClass(),
                        methodName,
                        ReflectionCache.EMPTY_CLASS_ARRAY,
                        ReflectionCache.EMPTY_OBJECT_ARRAY);
                result = mwa.cached.invoke(target, mwa.args);
              }
            } else {
              // Convert FlatBuffer arguments to Java objects
              Object[] javaArgs = new Object[argCount];
              Class<?>[] argTypes = new Class<?>[argCount];

              for (int i = 0; i < argCount; i++) {
                Argument arg = cmd.args(i);
                Object[] converted = convertArgument(arg);
                javaArgs[i] = converted[0];
                argTypes[i] = (Class<?>) converted[1];
              }

              // Find and invoke method via MethodHandle (faster than reflection)
              MethodWithArgs mwa =
                  MethodResolver.findMethodWithArgs(
                      target.getClass(), methodName, argTypes, javaArgs);
              result = mwa.cached.invoke(target, mwa.args);
            }

            // Wrap returned objects in registry
            if (result != null
                && (cmd.returnObjectRef() || !MethodResolver.isAutoConvertible(result))) {
              quota.allocateObject();
              long newId = objectIdCounter.getAndIncrement();
              putObject(newId, result);
              sessionObjectIds.add(newId);
              result = new ObjectRefT(newId);
            }
          } else if (cmd.action() == Action.InvokeStaticMethod) {
            // target_name format: "fully.qualified.ClassName.methodName"
            String fullName = cmd.targetName();
            int lastDot = fullName.lastIndexOf('.');
            if (lastDot == -1) {
              throw new IllegalArgumentException("Invalid static method format: " + fullName);
            }
            String className = fullName.substring(0, lastDot);
            String methodName = fullName.substring(lastDot + 1);

            requireClassAllowed(className, methodName);

            Class<?> clazz = getClass(className);

            // Convert FlatBuffer arguments to Java objects
            int argCount = cmd.argsLength();
            Object[] javaArgs = new Object[argCount];
            Class<?>[] argTypes = new Class<?>[argCount];

            for (int i = 0; i < argCount; i++) {
              Argument arg = cmd.args(i);
              Object[] converted = convertArgument(arg);
              javaArgs[i] = converted[0];
              argTypes[i] = (Class<?>) converted[1];
            }

            // Find and invoke static method via MethodHandle (faster than reflection)
            MethodWithArgs mwa =
                MethodResolver.findMethodWithArgs(clazz, methodName, argTypes, javaArgs);
            result = mwa.cached.invoke(null, mwa.args);

            // Wrap returned objects in registry
            // If returnObjectRef is true, always wrap as ObjectRef (no auto-conversion)
            if (result != null
                && (cmd.returnObjectRef() || !MethodResolver.isAutoConvertible(result))) {
              quota.allocateObject();
              long newId = objectIdCounter.getAndIncrement();
              putObject(newId, result);
              sessionObjectIds.add(newId);
              result = new ObjectRefT(newId);
            }
          } else if (cmd.action() == Action.GetField) {
            long targetId = cmd.targetId();
            Object target = sessionObject(targetId);
            String fieldName = cmd.targetName();

            if (target == null) throw new RuntimeException("Object " + targetId + " not found");
            allowlist.checkInstanceMember(target, fieldName);

            // Try VarHandle for faster access
            java.lang.invoke.VarHandle vh = getInstanceVarHandle(target.getClass(), fieldName);
            if (vh != null) {
              result = vh.get(target);
            } else {
              // Fallback to Field
              java.lang.reflect.Field field = getInstanceField(target.getClass(), fieldName);
              field.setAccessible(true);
              result = field.get(target);
            }

            // Wrap returned objects in registry
            if (result != null
                && (cmd.returnObjectRef() || !MethodResolver.isAutoConvertible(result))) {
              quota.allocateObject();
              long newId = objectIdCounter.getAndIncrement();
              putObject(newId, result);
              sessionObjectIds.add(newId);
              result = new ObjectRefT(newId);
            }
          } else if (cmd.action() == Action.SetField) {
            long targetId = cmd.targetId();
            Object target = sessionObject(targetId);
            String fieldName = cmd.targetName();

            if (target == null) throw new RuntimeException("Object " + targetId + " not found");
            allowlist.checkInstanceMember(target, fieldName);

            if (cmd.argsLength() != 1)
              throw new IllegalArgumentException("SetField requires exactly one argument");

            Argument arg = cmd.args(0);
            Object[] converted = convertArgument(arg);
            Object value = converted[0];

            // Try VarHandle for faster access
            java.lang.invoke.VarHandle vh = getInstanceVarHandle(target.getClass(), fieldName);
            if (vh != null) {
              vh.set(target, value);
            } else {
              // Fallback to Field
              java.lang.reflect.Field field = getInstanceField(target.getClass(), fieldName);
              field.setAccessible(true);
              field.set(target, value);
            }
            result = null;
          }
          // --- ARROW BLOCK ---
          else if (cmd.action() == Action.SendArrowBatch) {
            LOG.fine("Processing Arrow batch...");
            long payloadSize = this.responseOffset - PAYLOAD_OFFSET;
            MemorySegment payloadSlice = sharedMem.asSlice(PAYLOAD_OFFSET, payloadSize);
            ArrowMemoryHandler.Received received = arrowHandler.processArrowIpcBatch(payloadSlice);
            // The stream's length is known only once it is read. The client checks before it
            // writes; this is for one that did not.
            if (loans.overlaps(0, received.totalBytes())) {
              throw new IllegalStateException(
                  "Arrow stream at offset 0 overlaps a table that is lent to Java. End the loan"
                      + " first.");
            }
            observer.onArrowTransfer(
                "python_to_java", received.numRows(), received.totalBytes(), false);
            result = "Received " + received.numRows() + " rows";
          }
          // --- ZERO-COPY ARROW BUFFERS ---
          else if (cmd.action() == Action.SendArrowBuffers) {
            ArrowBatchDescriptor batchDesc = cmd.arrowBatch();
            if (batchDesc == null) {
              throw new IllegalArgumentException(
                  "SendArrowBuffers requires arrow_batch descriptor");
            }

            // For now, payload shm is the same as control shm (payloadSlice)
            // TODO: Support separate payload shm file
            long payloadSize = this.responseOffset - PAYLOAD_OFFSET;
            MemorySegment payloadSlice = sharedMem.asSlice(PAYLOAD_OFFSET, payloadSize);
            requireClearOfLoans(batchDesc, loans);

            // Loading wraps the buffers in place, so it is cheap to do before the quota check;
            // charging only once the batch is accepted keeps rejected sends from using up quota.
            ArrowMemoryHandler.Received received =
                arrowHandler.processArrowBuffers(batchDesc, payloadSlice);
            try {
              quota.allocateArrowBytes(received.totalBytes());
            } catch (RuntimeException e) {
              arrowHandler.close(); // over quota: do not keep the batch
              throw e;
            }
            observer.onArrowTransfer(
                "python_to_java", received.numRows(), received.totalBytes(), true);
            result = "Received " + received.numRows() + " rows via zero-copy buffers";
          }
          // --- RESET PAYLOAD ARENA ---
          else if (cmd.action() == Action.ResetPayloadArena) {
            LOG.fine("Payload arena reset requested");
            arrowHandler.reset();
            // The client is about to reuse the whole zone
            loans.endAll();
            // Reset Arrow buffer quota when arena is reset
            quota.resetArrowBytes();
            result = true;
          }
          // --- GET ARROW DATA (Java -> Python) ---
          else if (cmd.action() == Action.GetArrowData) {
            VectorSchemaRoot currentRoot = arrowHandler.getCurrentRoot();
            if (currentRoot == null) {
              throw new RuntimeException("No Arrow data available");
            }
            // Write Arrow buffers to payload zone
            long payloadSize = this.responseOffset - PAYLOAD_OFFSET;
            MemorySegment payloadSlice = sharedMem.asSlice(PAYLOAD_OFFSET, payloadSize);
            if (loans.overlaps(payloadSize / 2, payloadSize)) {
              throw new IllegalStateException(
                  "Arrow data cannot be returned while a table lent to Java extends past the first"
                      + " half of the payload zone, where returned data is written. End the loan"
                      + " first, or connect with more memory.");
            }
            ArrowMemoryHandler.ArrowWriteResult writeResult =
                arrowHandler.writeArrowBuffers(currentRoot, payloadSlice, null);
            responseOffset = packArrowResponse(builder, writeResult);
            responsePacked = true;
            observer.onArrowTransfer(
                "java_to_python", writeResult.numRows, writeResult.totalBytesWritten, false);
          }
          // --- ARROW TABLE AS A JAVA OBJECT (Python -> Java) ---
          else if (cmd.action() == Action.ArrowToObject) {
            long payloadSize = this.responseOffset - PAYLOAD_OFFSET;
            result =
                arrowToObject(
                    cmd.arrowStream(),
                    cmd.arrowForm(),
                    sharedMem.asSlice(PAYLOAD_OFFSET, payloadSize),
                    arrowHandler,
                    loans,
                    sessionObjectIds,
                    quota);
          }
          // --- ARROW TABLE HELD BY A JAVA OBJECT (Java -> Python) ---
          else if (cmd.action() == Action.ArrowFromObject) {
            // Everything past the batch loaded by send_arrow_buffers (if any) and the tables
            // the client has lent is free to use
            long payloadSize = this.responseOffset - PAYLOAD_OFFSET;
            long start =
                Math.min(
                    Math.max(arrowHandler.getLoadedEnd(), ArrowMemoryHandler.aligned(loans.end())),
                    payloadSize);
            long length =
                arrowFromObject(
                    cmd.targetId(),
                    sharedMem.asSlice(PAYLOAD_OFFSET + start, payloadSize - start),
                    arrowHandler,
                    quota.getSessionId());
            responseOffset = packArrowStreamResponse(builder, start, length, 0);
            responsePacked = true;
          }
          // --- CALLBACK BLOCK ---
          else if (cmd.action() == Action.RegisterCallback) {
            result = registerCallback(cmd, ctx);
          } else if (cmd.action() == Action.UnregisterCallback) {
            long unregistered = unregisterCallback(cmd.targetId(), sessionCallbackIds);
            result = unregistered == 0 ? null : unregistered;
          }
          // Note: CallbackResponse is handled directly in invokeCallback(), not here
          else if (cmd.action() == Action.OpenCallbackChannel) {
            result = openCallbackListener(ctx).toString();
          } else if (cmd.action() == Action.IsInstanceOf) {
            // Check if an object is an instance of a class
            long targetId = cmd.targetId();
            String className = cmd.targetName();

            Object target = sessionObject(targetId);
            if (target == null) throw new RuntimeException("Object " + targetId + " not found");

            // Try to load the class (no allowlist check - this is read-only)
            Class<?> clazz = getClass(className);
            result = clazz.isInstance(target);
          } else if (cmd.action() == Action.GetStaticField) {
            // Get static field: target_name = "pkg.Class.FIELD"
            String fullName = cmd.targetName();
            int lastDot = fullName.lastIndexOf('.');
            if (lastDot == -1) {
              throw new IllegalArgumentException("Invalid static field format: " + fullName);
            }
            String className = fullName.substring(0, lastDot);
            String fieldName = fullName.substring(lastDot + 1);

            requireClassAllowed(className, fieldName);

            Class<?> clazz = getClass(className);

            // Try VarHandle for faster static field access
            ReflectionCache.CachedStaticVarHandle svh = getStaticVarHandle(clazz, fieldName);
            if (svh != null) {
              result = svh.handle.get();
            } else {
              // Fallback to Field
              java.lang.reflect.Field field = getStaticField(clazz, fieldName);
              result = field.get(null);
            }

            // Wrap returned objects in registry
            if (result != null
                && (cmd.returnObjectRef() || !MethodResolver.isAutoConvertible(result))) {
              quota.allocateObject();
              long newId = objectIdCounter.getAndIncrement();
              putObject(newId, result);
              sessionObjectIds.add(newId);
              result = new ObjectRefT(newId);
            }
          } else if (cmd.action() == Action.SetStaticField) {
            // Set static field: target_name = "pkg.Class.FIELD", args[0] = value
            String fullName = cmd.targetName();
            int lastDot = fullName.lastIndexOf('.');
            if (lastDot == -1) {
              throw new IllegalArgumentException("Invalid static field format: " + fullName);
            }
            String className = fullName.substring(0, lastDot);
            String fieldName = fullName.substring(lastDot + 1);

            requireClassAllowed(className, fieldName);

            if (cmd.argsLength() != 1) {
              throw new IllegalArgumentException("SetStaticField requires exactly one argument");
            }

            Argument arg = cmd.args(0);
            Object[] converted = convertArgument(arg);
            Object value = converted[0];

            Class<?> clazz = getClass(className);

            // Try VarHandle for faster static field access
            ReflectionCache.CachedStaticVarHandle svh = getStaticVarHandle(clazz, fieldName);
            if (svh != null) {
              svh.handle.set(value);
            } else {
              // Fallback to Field
              java.lang.reflect.Field field = getStaticField(clazz, fieldName);
              field.set(null, value);
            }
            result = null;
          } else if (cmd.action() == Action.Reflect) {
            result = reflect(cmd.targetName());
          } else if (cmd.action() == Action.GetFields) {
            vectorResults = new ArrayList<>();
            getFields(cmd, sessionObjectIds, quota, vectorResults);
            result = vectorResults; // Will be packed as ListVal

          } else if (cmd.action() == Action.InvokeMethods) {
            vectorResults = new ArrayList<>();
            invokeMethods(cmd, sessionObjectIds, quota, vectorResults);
            result = vectorResults; // Will be packed as ListVal

          } else if (cmd.action() == Action.CreateObjects) {
            vectorResults = new ArrayList<>();
            createObjects(cmd, sessionObjectIds, quota, vectorResults);
            result = vectorResults; // Will be packed as ListVal

          } else if (cmd.action() == Action.Batch) {
            responseOffset =
                runBatch(
                    cmd.batch(),
                    builder,
                    sessionObjectIds,
                    sessionCallbackIds,
                    quota,
                    packedIds,
                    0);
            responsePacked = true;

          } else if (cmd.action() == Action.GetMetrics) {
            // Return server metrics as a string report
            result = Metrics.getInstance().report();
          } else if (cmd.action() == Action.Ping) {
            // Health check - just return true to confirm server is alive
            result = true;
          } else {
            throw new UnsupportedOperationException("Unknown action: " + cmd.action());
          }

          // 3. Pack Success, unless the action built its own response
          if (!responsePacked) {
            responseOffset = packResult(builder, result, sessionObjectIds, quota, packedIds);
          }

        } catch (Throwable t) {
          // Unwrap InvocationTargetException to get the real cause
          Throwable cause = unwrap(t);

          if (cause instanceof NoSuchMethodException
              || cause instanceof IllegalArgumentException
              || cause instanceof SecurityException
              || cause instanceof NoSuchFieldException) {
            LOG.log(Level.FINE, "Client error: " + cause.getMessage());
          } else {
            LOG.log(Level.WARNING, "Error processing command", cause);
          }

          errorMessage = formatException(cause);
          errorType = cause.getClass().getName();
          requestSuccess = false;
          responseOffset = packError(builder, errorMessage, errorType);
          // The client gets the error, not the results gathered before it
          discardResultRefs(vectorResults, sessionObjectIds, quota);
        } finally {
          // Idle time runs from the end of a request, not its start: one that took longer
          // than the timeout must not leave its session looking idle the moment it ends
          quota.touchActivity();
          control.endRequest();
        }

        // 4. Write Response to Zone C
        builder.finish(responseOffset);
        ByteBuffer resBuf = builder.dataBuffer();
        int resSize = resBuf.remaining();

        // Validate response size fits in zone
        if (resSize > RESPONSE_ZONE_SIZE) {
          // Response too large - send error instead. The client never sees the IDs in the
          // response it was meant to get, so it cannot free them.
          discardObjects(packedIds, sessionObjectIds, quota);
          if (requestSuccess) {
            discardResultRefs(vectorResults, sessionObjectIds, quota);
          }
          builder = new FlatBufferBuilder(256);
          errorMessage =
              new PayloadTooLargeException("Response", resSize, RESPONSE_ZONE_SIZE).getMessage();
          errorType = PayloadTooLargeException.class.getName();
          requestSuccess = false;
          responseOffset = packError(builder, errorMessage, errorType);
          builder.finish(responseOffset);
          resBuf = builder.dataBuffer();
          resSize = resBuf.remaining();
        }

        // Record request completion in observer
        observer.onRequestEnd(requestSuccess, errorType, errorMessage);

        MemorySegment responseSlice = sharedMem.asSlice(this.responseOffset, resSize);
        responseSlice.copyFrom(MemorySegment.ofBuffer(resBuf));

        // 5. Signal Python
        lengthBuf.putInt(resSize);
        lengthBuf.flip();
        client.write(lengthBuf);
        lengthBuf.clear();

        currentRequestId.remove();
      }
    } catch (IOException e) {
      // Losing the connection is how sessions normally end; failing to set one up is not
      LOG.log(
          sessions.containsKey(sessionShmPath.toString()) ? Level.FINE : Level.WARNING,
          "Client session " + sessionId + " ended: " + e.getMessage());
    } catch (RuntimeException | Error e) {
      // Nothing waits on this task's result, so an unexpected failure (during session setup,
      // say) would otherwise close the connection without a trace
      LOG.log(Level.WARNING, "Client session " + sessionId + " failed", e);
    } finally {
      // The client takes the close as its answer when a session is refused, and may stop the
      // server at once to report it: the reason has to be in the log by now.
      try {
        client.close();
      } catch (IOException e) {
        // already gone
      }
      closeCallbackConnection(sessionContext.get());

      // Notify observer of session end
      observer.onSessionEnd(quota.getTotalObjectsCreated());

      // Log quota summary
      LOG.fine(quota.getSummary());

      // Cleanup orphans: remove session objects and callbacks from global registries
      for (Long id : sessionObjectIds) removeObject(id);
      for (Long id : sessionCallbackIds) callbackRegistry.remove(id);
      // After the objects: that ended the loans the registry knew of
      loans.close();
      sessions.remove(sessionShmPath.toString());
      sessionCount.decrementAndGet();
      sessionContext.remove();
      currentRequestId.remove();
      argumentNestingDepth.remove();
      // Clean up session shared memory file
      try {
        Files.deleteIfExists(sessionShmPath);
        LOG.fine("Session " + sessionId + " SHM file deleted: " + sessionShmPath);
      } catch (IOException e) {
        LOG.warning("Failed to delete session SHM file: " + sessionShmPath);
      }
      sessionFiles.remove(sessionShmPath);
    }
  }

  /**
   * Check if the current request has been cancelled. Can be called from long-running operations to
   * support cooperative cancellation.
   *
   * @throws InterruptedException if the current request was cancelled
   */
  public static void checkCancelled() throws InterruptedException {
    if (isCancelled()) {
      throw new InterruptedException("Request was cancelled");
    }
  }

  /**
   * Check if the current request has been cancelled without throwing.
   *
   * @return true if the current request was cancelled
   */
  public static boolean isCancelled() {
    // A cancel is for the request the session's own thread is running, not for whatever a
    // callback on another thread has that thread do for the session meanwhile
    SessionContext ctx = sessionContext.get();
    return Thread.currentThread().isInterrupted()
        || (ctx != null && ctx.control.isSessionThread() && ctx.control.isCancelled());
  }

  /**
   * The exception the called code threw, without the wrappers reflection and proxies put around it:
   * a checked exception thrown through a callback proxy arrives as an UndeclaredThrowableException.
   */
  private static Throwable unwrap(Throwable t) {
    while ((t instanceof java.lang.reflect.InvocationTargetException
            || t instanceof java.lang.reflect.UndeclaredThrowableException)
        && t.getCause() != null) {
      t = t.getCause();
    }
    return t;
  }

  /** Answer a request that could not be run with an error, in place of its result. */
  private void writeErrorResponse(
      SocketChannel client, MemorySegment sharedMem, String message, String errorType)
      throws IOException {
    FlatBufferBuilder builder = new FlatBufferBuilder(256);
    builder.finish(packError(builder, message, errorType));
    ByteBuffer response = builder.dataBuffer();
    int size = response.remaining();
    sharedMem.asSlice(this.responseOffset, size).copyFrom(MemorySegment.ofBuffer(response));
    ByteBuffer length = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    length.putInt(size).flip();
    while (length.hasRemaining()) {
      client.write(length);
    }
  }

  // A command is at most 64KB, so no vector in it has more elements than that. The counts are
  // read from the command itself, and a forged one would otherwise size an allocation.
  private static void checkArgumentCount(int count) {
    if (count < 0 || count > PAYLOAD_OFFSET) {
      throw new IllegalArgumentException("Argument count " + count + " is not possible");
    }
  }

  private static int arrayLength(int length) {
    if (length < 0 || length > PAYLOAD_OFFSET) {
      throw new IllegalArgumentException("Array length " + length + " is not possible");
    }
    return length;
  }

  /**
   * Handle FreeObject. The clients send the IDs in free_ids; one ID in target_id, or several as
   * integer args with target_id ignored, are accepted as well. Unknown and already-freed IDs are
   * ignored.
   */
  private void freeObjects(Command cmd, Set<Long> sessionObjectIds, SessionQuota quota) {
    freeRiding(cmd, sessionObjectIds, quota);
    int idCount = cmd.argsLength();
    if (idCount == 0) {
      freeObject(cmd.targetId(), sessionObjectIds, quota);
      return;
    }
    IntVal iv = tempIntVal.get();
    for (int i = 0; i < idCount; i++) {
      Argument arg = cmd.args(i);
      if (arg.valType() == Value.IntVal) {
        arg.val(iv);
        freeObject(iv.v(), sessionObjectIds, quota);
      }
    }
  }

  /**
   * Release the objects a command names in {@code free_ids}. Any command may carry them: a handle
   * the client dropped is released with its next command instead of a round trip of its own.
   */
  private void freeRiding(Command cmd, Set<Long> sessionObjectIds, SessionQuota quota) {
    for (int i = 0, count = cmd.freeIdsLength(); i < count; i++) {
      freeObject(cmd.freeIds(i), sessionObjectIds, quota);
    }
  }

  private void freeObject(long objectId, Set<Long> sessionObjectIds, SessionQuota quota) {
    if (sessionObjectIds.remove(objectId)) {
      removeObject(objectId);
      quota.releaseObject();
    }
  }

  /**
   * The object an ID from the client refers to, or null if the calling session has no such object.
   * IDs come from one counter shared by all sessions and are easy to guess, so a lookup that
   * skipped this check would let one session use another's objects.
   */
  private Object sessionObject(long objectId) {
    SessionContext ctx = sessionContext.get();
    if (ctx != null && !ctx.sessionObjectIds.contains(objectId)) {
      return null;
    }
    return objectRegistry.get(objectId);
  }

  /**
   * An object passed as an argument. A reference to a freed or unknown object is an error: Python
   * sends None as a null value, never as an object reference.
   */
  private Object requireSessionObject(long objectId) {
    Object obj = sessionObject(objectId);
    if (obj == null) {
      throw new IllegalArgumentException(
          "Object " + objectId + " not found: it was freed or belongs to another session");
    }
    return obj;
  }

  /** Add an object to the registry. All registrations go through here so they are observed. */
  private void putObject(long objectId, Object obj) {
    objectRegistry.put(objectId, obj);
    synchronized (ownedTables) {
      OwnedTable owned = ownedTables.get(obj);
      if (owned != null) {
        owned.references++;
      }
    }
    observeRegistry(objectId, "add", obj.getClass().getName(), 1);
  }

  /** Remove an object from the registry; unknown IDs are ignored. */
  private void removeObject(long objectId) {
    Object removed = objectRegistry.remove(objectId);
    if (removed != null) {
      observeRegistry(objectId, "remove", null, -1);
      releaseOwnedTable(removed);
      ArrowLoans.Loan loan = lentTables.remove(objectId);
      if (loan != null) {
        loan.end();
      }
    }
  }

  /**
   * Close a table made by ArrowToObject once its last registry entry is gone. The same table can be
   * registered under several IDs, for example after a Java method returned it again.
   */
  private void releaseOwnedTable(Object obj) {
    OwnedTable owned;
    synchronized (ownedTables) {
      owned = ownedTables.get(obj);
      if (owned == null || --owned.references > 0) {
        return;
      }
      ownedTables.remove(obj);
    }
    owned.quota.releaseHeldArrowBytes(owned.bytes);
    try {
      ((VectorSchemaRoot) obj).close();
    } catch (RuntimeException e) {
      LOG.log(Level.WARNING, "Failed to close an Arrow table", e);
    }
  }

  private void observeRegistry(long objectId, String operation, String objectType, int delta) {
    Metrics.getInstance().updateObjectCount(delta);
    // Not set on a foreign thread that invokes a callback proxy; those report session 0
    SessionContext ctx = sessionContext.get();
    long sessionId = ctx != null ? ctx.quota.getSessionId() : 0;
    int registrySize = objectRegistry.size();
    StructuredLogger.logObjectRegistry(sessionId, objectId, operation, objectType, registrySize);
    GatunEvents.emitObjectRegistry(sessionId, objectId, operation, objectType, registrySize);
  }

  /**
   * Free the objects a vectorized action registered for results the client will not receive.
   * Results that are not object references are skipped.
   */
  private void discardResultRefs(
      java.util.List<Object> results, Set<Long> sessionObjectIds, SessionQuota quota) {
    if (results == null) {
      return;
    }
    for (Object item : results) {
      if (item instanceof ObjectRefT ref) {
        freeObject(ref.id, sessionObjectIds, quota);
      }
    }
  }

  /** Free objects that were registered for a result the client will not receive. */
  private void discardObjects(List<Long> ids, Set<Long> sessionObjectIds, SessionQuota quota) {
    for (long id : ids) {
      freeObject(id, sessionObjectIds, quota);
    }
  }

  /**
   * Pack a result for the session. Objects nested in the result (the elements of a returned list,
   * say) are registered while packing; they are counted against the quota here because the client
   * frees them like any other object. If packing fails, the client never learns their IDs, so they
   * are dropped again. Otherwise their IDs are added to {@code registered}, for a caller that may
   * still have to drop them.
   */
  private int packResult(
      FlatBufferBuilder builder,
      Object result,
      Set<Long> sessionObjectIds,
      SessionQuota quota,
      List<Long> registered) {
    return packResult(builder, result, sessionObjectIds, quota, registered, 0);
  }

  /**
   * @param callId for a response on the callback connection, the call it belongs to; 0 otherwise
   */
  private int packResult(
      FlatBufferBuilder builder,
      Object result,
      Set<Long> sessionObjectIds,
      SessionQuota quota,
      List<Long> registered,
      long callId) {
    List<Long> ids = new ArrayList<>();
    // Packing can itself call Python (iterating a collection that a callback implements), and
    // that call packs its arguments on this thread
    List<Long> outer = packRecorder.get();
    packRecorder.set(ids);
    boolean packed = false;
    try {
      int offset = packSuccessWithSession(builder, result, sessionObjectIds, callId);
      packed = true;
      return offset;
    } finally {
      packRecorder.set(outer);
      quota.countRegisteredObjects(ids.size());
      if (!packed) {
        discardObjects(ids, sessionObjectIds, quota);
      } else if (registered != null) {
        registered.addAll(ids);
      }
    }
  }

  /**
   * Execute a single command and return the result.
   *
   * <p>This method is used by batch processing to execute sub-commands. It handles all action types
   * except Batch (to prevent recursion) and Cancel (which is handled specially).
   *
   * @param cmd The command to execute
   * @param sessionObjectIds Set of object IDs owned by this session (for cleanup)
   * @param sessionCallbackIds Set of callback IDs owned by this session
   * @param sharedMem Shared memory segment for Arrow operations
   * @param quota Session quota tracker for resource limits
   * @return The result object (may be null, primitive, String, ObjectRefT, etc.)
   * @throws Throwable Any exception from command execution
   */
  private Object executeCommand(
      Command cmd,
      Set<Long> sessionObjectIds,
      Set<Long> sessionCallbackIds,
      MemorySegment sharedMem,
      SessionQuota quota)
      throws Throwable {

    // Disallow nested batch commands
    if (cmd.action() == Action.Batch) {
      throw new IllegalArgumentException("Nested batch commands are not supported");
    }

    // Disallow Cancel in batch (should be handled at top level)
    if (cmd.action() == Action.Cancel) {
      throw new IllegalArgumentException("Cancel action cannot be used in batch");
    }

    checkArgumentCount(cmd.argsLength());
    Object result = null;

    if (cmd.action() == Action.CreateObject) {
      String className = cmd.targetName();
      requireClassAllowed(className);

      // Refuse before running the constructor; the slot is taken once it succeeds
      quota.checkObjectLimit();

      Class<?> clazz = getClass(className);

      int argCount = cmd.argsLength();
      Object instance;

      if (argCount == 0) {
        instance = getNoArgConstructor(clazz).newInstance();
      } else {
        Object[] javaArgs = new Object[argCount];
        Class<?>[] argTypes = new Class<?>[argCount];

        for (int i = 0; i < argCount; i++) {
          Argument arg = cmd.args(i);
          Object[] converted = convertArgument(arg);
          javaArgs[i] = converted[0];
          argTypes[i] = (Class<?>) converted[1];
        }

        var ctorResult = MethodResolver.findConstructorWithArgs(clazz, argTypes, javaArgs);
        instance = ctorResult.cached.newInstance(ctorResult.args);
      }

      quota.allocateObject();
      long newId = objectIdCounter.getAndIncrement();
      putObject(newId, instance);
      sessionObjectIds.add(newId);
      result = new ObjectRefT(newId);

    } else if (cmd.action() == Action.FreeObject) {
      freeObjects(cmd, sessionObjectIds, quota);
      result = null;

    } else if (cmd.action() == Action.InvokeMethod) {
      long targetId = cmd.targetId();
      Object target = sessionObject(targetId);
      String methodName = cmd.targetName();

      if (target == null) throw new RuntimeException("Object " + targetId + " not found");
      allowlist.checkInstanceMember(target, methodName);

      int argCount = cmd.argsLength();

      if (argCount == 0) {
        MethodHandle handle = getNoArgMethodHandle(target.getClass(), methodName);
        if (handle != null) {
          result = handle.invoke(target);
        } else {
          MethodWithArgs mwa =
              MethodResolver.findMethodWithArgs(
                  target.getClass(),
                  methodName,
                  ReflectionCache.EMPTY_CLASS_ARRAY,
                  ReflectionCache.EMPTY_OBJECT_ARRAY);
          result = mwa.cached.invoke(target, mwa.args);
        }
      } else {
        Object[] javaArgs = new Object[argCount];
        Class<?>[] argTypes = new Class<?>[argCount];

        for (int i = 0; i < argCount; i++) {
          Argument arg = cmd.args(i);
          Object[] converted = convertArgument(arg);
          javaArgs[i] = converted[0];
          argTypes[i] = (Class<?>) converted[1];
        }

        MethodWithArgs mwa =
            MethodResolver.findMethodWithArgs(target.getClass(), methodName, argTypes, javaArgs);
        result = mwa.cached.invoke(target, mwa.args);
      }

      // Wrap returned objects in registry
      if (result != null && (cmd.returnObjectRef() || !MethodResolver.isAutoConvertible(result))) {
        quota.allocateObject();
        long newId = objectIdCounter.getAndIncrement();
        putObject(newId, result);
        sessionObjectIds.add(newId);
        result = new ObjectRefT(newId);
      }

    } else if (cmd.action() == Action.InvokeStaticMethod) {
      String fullName = cmd.targetName();
      int lastDot = fullName.lastIndexOf('.');
      if (lastDot == -1) {
        throw new IllegalArgumentException("Invalid static method format: " + fullName);
      }
      String className = fullName.substring(0, lastDot);
      String methodName = fullName.substring(lastDot + 1);

      requireClassAllowed(className, methodName);

      Class<?> clazz = getClass(className);

      int argCount = cmd.argsLength();
      Object[] javaArgs = new Object[argCount];
      Class<?>[] argTypes = new Class<?>[argCount];

      for (int i = 0; i < argCount; i++) {
        Argument arg = cmd.args(i);
        Object[] converted = convertArgument(arg);
        javaArgs[i] = converted[0];
        argTypes[i] = (Class<?>) converted[1];
      }

      MethodWithArgs mwa = MethodResolver.findMethodWithArgs(clazz, methodName, argTypes, javaArgs);
      result = mwa.cached.invoke(null, mwa.args);

      // Wrap returned objects in registry
      if (result != null && (cmd.returnObjectRef() || !MethodResolver.isAutoConvertible(result))) {
        quota.allocateObject();
        long newId = objectIdCounter.getAndIncrement();
        putObject(newId, result);
        sessionObjectIds.add(newId);
        result = new ObjectRefT(newId);
      }

    } else if (cmd.action() == Action.GetField) {
      long targetId = cmd.targetId();
      Object target = sessionObject(targetId);
      String fieldName = cmd.targetName();

      if (target == null) throw new RuntimeException("Object " + targetId + " not found");
      allowlist.checkInstanceMember(target, fieldName);

      // Try VarHandle for faster access
      java.lang.invoke.VarHandle vh = getInstanceVarHandle(target.getClass(), fieldName);
      if (vh != null) {
        result = vh.get(target);
      } else {
        java.lang.reflect.Field field = getInstanceField(target.getClass(), fieldName);
        field.setAccessible(true);
        result = field.get(target);
      }

      if (result != null && (cmd.returnObjectRef() || !MethodResolver.isAutoConvertible(result))) {
        quota.allocateObject();
        long newId = objectIdCounter.getAndIncrement();
        putObject(newId, result);
        sessionObjectIds.add(newId);
        result = new ObjectRefT(newId);
      }

    } else if (cmd.action() == Action.SetField) {
      long targetId = cmd.targetId();
      Object target = sessionObject(targetId);
      String fieldName = cmd.targetName();

      if (target == null) throw new RuntimeException("Object " + targetId + " not found");
      allowlist.checkInstanceMember(target, fieldName);

      if (cmd.argsLength() != 1)
        throw new IllegalArgumentException("SetField requires exactly one argument");

      Argument arg = cmd.args(0);
      Object[] converted = convertArgument(arg);
      Object value = converted[0];

      // Try VarHandle for faster access
      java.lang.invoke.VarHandle vh = getInstanceVarHandle(target.getClass(), fieldName);
      if (vh != null) {
        vh.set(target, value);
      } else {
        java.lang.reflect.Field field = getInstanceField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
      }
      result = null;

    } else if (cmd.action() == Action.IsInstanceOf) {
      long targetId = cmd.targetId();
      Object target = sessionObject(targetId);
      String className = cmd.targetName();

      if (target == null) throw new RuntimeException("Object " + targetId + " not found");

      Class<?> clazz = getClass(className);
      result = clazz.isInstance(target);

    } else if (cmd.action() == Action.GetStaticField) {
      String fullName = cmd.targetName();
      int lastDot = fullName.lastIndexOf('.');
      if (lastDot == -1) {
        throw new IllegalArgumentException("Invalid static field format: " + fullName);
      }
      String className = fullName.substring(0, lastDot);
      String fieldName = fullName.substring(lastDot + 1);

      requireClassAllowed(className, fieldName);

      Class<?> clazz = getClass(className);

      // Try VarHandle for faster access
      ReflectionCache.CachedStaticVarHandle svh = getStaticVarHandle(clazz, fieldName);
      if (svh != null) {
        result = svh.handle.get();
      } else {
        java.lang.reflect.Field field = getStaticField(clazz, fieldName);
        result = field.get(null);
      }

      if (result != null && (cmd.returnObjectRef() || !MethodResolver.isAutoConvertible(result))) {
        quota.allocateObject();
        long newId = objectIdCounter.getAndIncrement();
        putObject(newId, result);
        sessionObjectIds.add(newId);
        result = new ObjectRefT(newId);
      }

    } else if (cmd.action() == Action.SetStaticField) {
      String fullName = cmd.targetName();
      int lastDot = fullName.lastIndexOf('.');
      if (lastDot == -1) {
        throw new IllegalArgumentException("Invalid static field format: " + fullName);
      }
      String className = fullName.substring(0, lastDot);
      String fieldName = fullName.substring(lastDot + 1);

      requireClassAllowed(className, fieldName);

      if (cmd.argsLength() != 1) {
        throw new IllegalArgumentException("SetStaticField requires exactly one argument");
      }

      Argument arg = cmd.args(0);
      Object[] converted = convertArgument(arg);
      Object value = converted[0];

      Class<?> clazz = getClass(className);

      // Try VarHandle for faster access
      ReflectionCache.CachedStaticVarHandle svh = getStaticVarHandle(clazz, fieldName);
      if (svh != null) {
        svh.handle.set(value);
      } else {
        java.lang.reflect.Field field = getStaticField(clazz, fieldName);
        field.set(null, value);
      }
      result = null;

    } else {
      // Actions not supported in batch: Arrow operations, callbacks, etc.
      throw new UnsupportedOperationException(
          "Action " + cmd.action() + " is not supported in batch mode");
    }

    return result;
  }

  // --- HELPER: Convert FlatBuffer Argument to Java Object ---
  private Object[] convertArgument(Argument arg) {
    // Track nesting depth to prevent stack overflow from deeply nested structures
    int depth = argumentNestingDepth.get();
    ResourceLimits.checkNestingDepth(depth);
    argumentNestingDepth.set(depth + 1);

    try {
      return convertArgumentInternal(arg);
    } finally {
      argumentNestingDepth.set(depth);
    }
  }

  private Object[] convertArgumentInternal(Argument arg) {
    byte valType = arg.valType();
    Object value;
    Class<?> type;

    // Check for explicit type hint - used for overload resolution
    String typeHint = arg.typeHint();

    if (valType == Value.StringVal) {
      StringVal sv = (StringVal) arg.val(tempStringVal.get());
      value = sv.v();
      // Check string length limit
      ResourceLimits.checkStringLength((String) value);
      type = String.class; // Use String for better overload resolution
    } else if (valType == Value.IntVal) {
      IntVal iv = (IntVal) arg.val(tempIntVal.get());
      long longVal = iv.v();
      // Check if value fits in int range; if so use int for common Java APIs
      // Otherwise use long to avoid overflow (e.g., epoch milliseconds for Timestamp)
      if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
        value = (int) longVal;
        type = int.class;
      } else {
        value = longVal;
        type = long.class;
      }
    } else if (valType == Value.DoubleVal) {
      DoubleVal dv = (DoubleVal) arg.val(tempDoubleVal.get());
      value = dv.v();
      type = double.class;
    } else if (valType == Value.BoolVal) {
      BoolVal bv = (BoolVal) arg.val(tempBoolVal.get());
      value = bv.v();
      type = boolean.class;
    } else if (valType == Value.CharVal) {
      CharVal cv = (CharVal) arg.val(tempCharVal.get());
      value = (char) cv.v(); // Convert ushort to char
      type = char.class;
    } else if (valType == Value.ObjectRef) {
      ObjectRef ref = (ObjectRef) arg.val(tempObjectRef.get());
      value = requireSessionObject(ref.id());
      // Use actual object type for better overload resolution
      type = value.getClass();
    } else if (valType == Value.ListVal) {
      // One element type for a list of integers: Integer if every value fits, otherwise Long.
      // Choosing per element gave Java code a List that was neither.
      value = convertListVal((ListVal) arg.val(new ListVal()), true);
      type = java.util.ArrayList.class;
    } else if (valType == Value.MapVal) {
      MapVal mv = (MapVal) arg.val(new MapVal());
      List<Object> keys = mv.keys() == null ? List.of() : convertListVal(mv.keys(), false);
      List<Object> values = mv.values() == null ? List.of() : convertListVal(mv.values(), false);
      if (keys.size() != values.size()) {
        throw new IllegalArgumentException(
            "A map with " + keys.size() + " keys and " + values.size() + " values");
      }
      java.util.HashMap<Object, Object> map = new java.util.HashMap<>();
      for (int i = 0; i < keys.size(); i++) {
        map.put(keys.get(i), values.get(i));
      }
      value = map;
      type = java.util.HashMap.class;
    } else if (valType == Value.NullVal) {
      value = null;
      // Null cannot be assigned to primitive types
      // If type hint specifies a primitive, this is an error
      if (typeHint != null && !typeHint.isEmpty()) {
        Class<?> hintedType = resolveTypeHint(typeHint);
        if (hintedType.isPrimitive()) {
          throw new IllegalArgumentException("Cannot pass null to primitive type: " + typeHint);
        }
        type = hintedType;
        // Skip the type hint resolution below since we already handled it
        return new Object[] {value, type};
      }
      type = Object.class;
    } else if (valType == Value.ArrayVal) {
      // Convert ArrayVal to Java array
      ArrayVal av = (ArrayVal) arg.val(new ArrayVal());
      value = convertArrayVal(av);
      type = value.getClass();
    } else if (valType == Value.TemporalVal) {
      // Python datetime / date / time
      TemporalVal tv = (TemporalVal) arg.val(new TemporalVal());
      value = convertTemporalVal(tv.kind(), tv.v());
      type = value.getClass();
    } else if (valType == Value.BigIntVal) {
      // A Python int that does not fit in a long
      BigIntVal bv = (BigIntVal) arg.val(new BigIntVal());
      if (bv.vLength() == 0) {
        throw new IllegalArgumentException("A BigIntVal with no bytes");
      }
      ResourceLimits.checkByteLength(bv.vLength());
      byte[] twosComplement = new byte[bv.vLength()];
      bv.vAsByteBuffer().get(twosComplement);
      value = new java.math.BigInteger(twosComplement);
      type = java.math.BigInteger.class;
    } else if (valType == Value.DecimalVal) {
      // A Python decimal.Decimal
      DecimalVal dv = (DecimalVal) arg.val(new DecimalVal());
      String text = dv.v();
      ResourceLimits.checkStringLength(text);
      value = new java.math.BigDecimal(text);
      type = java.math.BigDecimal.class;
    } else {
      value = null;
      type = Object.class;
    }

    // If type hint is provided, use it for overload resolution instead of inferred type
    // The value is converted too: a Python int hinted as "long" or "short" has to arrive as that
    // type, or a method that takes Object or the boxed type gets an Integer.
    if (typeHint != null && !typeHint.isEmpty()) {
      type = resolveTypeHint(typeHint);
      value = MethodResolver.coerce(value, type);
    }

    return new Object[] {value, type};
  }

  /**
   * Resolve a type hint string to a Class object. Supports primitives, boxed types, and fully
   * qualified class names.
   */
  private Class<?> resolveTypeHint(String typeHint) {
    // Handle primitive types
    return switch (typeHint) {
      case "int" -> int.class;
      case "long" -> long.class;
      case "double" -> double.class;
      case "float" -> float.class;
      case "boolean" -> boolean.class;
      case "byte" -> byte.class;
      case "short" -> short.class;
      case "char" -> char.class;
      case "void" -> void.class;
      // Handle common boxed types (shortcuts)
      case "Integer", "java.lang.Integer" -> Integer.class;
      case "Long", "java.lang.Long" -> Long.class;
      case "Double", "java.lang.Double" -> Double.class;
      case "Float", "java.lang.Float" -> Float.class;
      case "Boolean", "java.lang.Boolean" -> Boolean.class;
      case "Byte", "java.lang.Byte" -> Byte.class;
      case "Short", "java.lang.Short" -> Short.class;
      case "Character", "java.lang.Character" -> Character.class;
      case "String", "java.lang.String" -> String.class;
      case "Object", "java.lang.Object" -> Object.class;
      // Handle common collection interfaces (for better overload resolution)
      case "List", "java.util.List" -> java.util.List.class;
      case "Map", "java.util.Map" -> java.util.Map.class;
      case "Set", "java.util.Set" -> java.util.Set.class;
      case "Collection", "java.util.Collection" -> java.util.Collection.class;
      case "Iterable", "java.lang.Iterable" -> Iterable.class;
      default -> {
        // Try to load class by fully qualified name
        // Through the cache, so classes added with connect(classpath=...) are found
        try {
          yield getClass(typeHint);
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("Type hint names an unknown class: " + typeHint);
        }
      }
    };
  }

  /**
   * The elements of a ListVal, whichever form it travels in.
   *
   * @param oneIntegerType for a list of integers: all Integer if every value fits in an int and all
   *     Long otherwise. Without it each value is an Integer if it fits, as a single integer
   *     argument is (the keys and values of a map).
   */
  private java.util.ArrayList<Object> convertListVal(ListVal lv, boolean oneIntegerType) {
    int len = lv.longsLength();
    if (len > 0) {
      ResourceLimits.checkCollectionSize(len);
      java.util.ArrayList<Object> list = new java.util.ArrayList<>(len);
      boolean allFit = true;
      for (int i = 0; i < len && allFit; i++) {
        allFit = fitsInt(lv.longs(i));
      }
      for (int i = 0; i < len; i++) {
        long v = lv.longs(i);
        if (allFit || (!oneIntegerType && fitsInt(v))) {
          list.add((int) v);
        } else {
          list.add(v);
        }
      }
      return list;
    }
    len = lv.doublesLength();
    if (len > 0) {
      ResourceLimits.checkCollectionSize(len);
      java.util.ArrayList<Object> list = new java.util.ArrayList<>(len);
      for (int i = 0; i < len; i++) {
        list.add(lv.doubles(i));
      }
      return list;
    }
    len = lv.boolsLength();
    if (len > 0) {
      ResourceLimits.checkCollectionSize(len);
      java.util.ArrayList<Object> list = new java.util.ArrayList<>(len);
      for (int i = 0; i < len; i++) {
        list.add(lv.bools(i));
      }
      return list;
    }
    len = lv.stringLengthsLength();
    if (len > 0) {
      ResourceLimits.checkCollectionSize(len);
      java.util.ArrayList<Object> list = new java.util.ArrayList<>(len);
      int total = lv.stringBytesLength();
      byte[] bytes = new byte[total];
      if (total > 0) {
        lv.stringBytesAsByteBuffer().get(bytes);
      }
      int at = 0;
      for (int i = 0; i < len; i++) {
        int n = lv.stringLengths(i);
        if (n < 0 || n > total - at) {
          throw new IllegalArgumentException("A list of strings whose lengths exceed its bytes");
        }
        String element = new String(bytes, at, n, java.nio.charset.StandardCharsets.UTF_8);
        ResourceLimits.checkStringLength(element);
        list.add(element);
        at += n;
      }
      return list;
    }
    len = lv.itemsLength();
    ResourceLimits.checkCollectionSize(len);
    java.util.ArrayList<Object> list = new java.util.ArrayList<>(len);
    for (int i = 0; i < len; i++) {
      list.add(convertArgument(lv.items(i))[0]);
    }
    if (oneIntegerType && len > 0) {
      // Integers that came one by one (an IntEnum, a subclass of int) follow the same rule,
      // unless a type hint chose the type of one of them
      boolean integers = true;
      boolean allFit = true;
      for (int i = 0; i < len && integers; i++) {
        Argument item = lv.items(i);
        integers = item.valType() == Value.IntVal && item.typeHint() == null;
        allFit &= list.get(i) instanceof Integer;
      }
      if (integers && !allFit) {
        list.replaceAll(element -> ((Number) element).longValue());
      }
    }
    return list;
  }

  private static boolean fitsInt(long v) {
    return v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE;
  }

  /** Convert a TemporalVal to the java.sql type that matches its kind. */
  private static Object convertTemporalVal(byte kind, long v) {
    if (kind == TemporalKind.Timestamp) {
      // v is microseconds since the epoch; floor so that pre-1970 values keep a positive
      // sub-second part, as Timestamp requires.
      java.sql.Timestamp ts = new java.sql.Timestamp(Math.floorDiv(v, 1_000_000L) * 1000L);
      ts.setNanos((int) Math.floorMod(v, 1_000_000L) * 1000);
      return ts;
    } else if (kind == TemporalKind.Date) {
      return java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(v));
    } else if (kind == TemporalKind.Time) {
      return java.sql.Time.valueOf(java.time.LocalTime.ofSecondOfDay(v));
    }
    throw new IllegalArgumentException("Unknown temporal kind: " + kind);
  }

  // --- HELPER: Convert ArrayVal to Java array ---
  private Object convertArrayVal(ArrayVal av) {
    byte elemType = av.elementType();

    if (elemType == ElementType.Int) {
      int len = arrayLength(av.intValuesLength());
      int[] arr = new int[len];
      for (int i = 0; i < len; i++) {
        arr[i] = av.intValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Long) {
      int len = arrayLength(av.longValuesLength());
      long[] arr = new long[len];
      for (int i = 0; i < len; i++) {
        arr[i] = av.longValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Double) {
      int len = arrayLength(av.doubleValuesLength());
      double[] arr = new double[len];
      for (int i = 0; i < len; i++) {
        arr[i] = av.doubleValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Float) {
      // Float was widened to double, narrow back
      int len = arrayLength(av.doubleValuesLength());
      float[] arr = new float[len];
      for (int i = 0; i < len; i++) {
        arr[i] = (float) av.doubleValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Bool) {
      int len = arrayLength(av.boolValuesLength());
      boolean[] arr = new boolean[len];
      for (int i = 0; i < len; i++) {
        arr[i] = av.boolValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Byte) {
      int len = arrayLength(av.byteValuesLength());
      byte[] arr = new byte[len];
      for (int i = 0; i < len; i++) {
        arr[i] = (byte) av.byteValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Short) {
      // Short was widened to int, narrow back
      int len = arrayLength(av.intValuesLength());
      short[] arr = new short[len];
      for (int i = 0; i < len; i++) {
        arr[i] = (short) av.intValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Char) {
      // Char was widened to int, narrow back
      int len = arrayLength(av.intValuesLength());
      char[] arr = new char[len];
      for (int i = 0; i < len; i++) {
        arr[i] = (char) av.intValues(i);
      }
      return arr;
    } else if (elemType == ElementType.String) {
      // String array stored as object_values
      int len = arrayLength(av.objectValuesLength());
      String[] arr = new String[len];
      for (int i = 0; i < len; i++) {
        Argument item = av.objectValues(i);
        Object[] converted = convertArgument(item);
        arr[i] = (String) converted[0];
      }
      return arr;
    } else {
      // Object array
      int len = arrayLength(av.objectValuesLength());
      Object[] arr = new Object[len];
      for (int i = 0; i < len; i++) {
        Argument item = av.objectValues(i);
        Object[] converted = convertArgument(item);
        arr[i] = converted[0];
      }
      return arr;
    }
  }

  // --- HELPER: Format exception with full stack trace ---
  // Longest error text sent to the client. A deep stack trace (Spark's run to hundreds of
  // frames) must not push the response past the 64KB response zone: the client would then get
  // a size error in place of the exception's type and message.
  private static final int MAX_ERROR_TEXT = 12_000;

  // Frames shown per exception in the chain when the whole trace does not fit
  private static final int MAX_FRAMES_WHEN_CUT = 12;

  private static String formatException(Throwable t) {
    String full = stackTrace(t, Integer.MAX_VALUE);
    if (full.length() <= MAX_ERROR_TEXT) {
      return full;
    }
    // Too long: shorten each exception's frames rather than cutting the end off, because the
    // end is where the root cause is ("Caused by: ..."), and that is the line that explains it
    String cut = stackTrace(t, MAX_FRAMES_WHEN_CUT);
    if (cut.length() > MAX_ERROR_TEXT) {
      cut = cut.substring(0, MAX_ERROR_TEXT) + "\n\t...";
    }
    return cut + "\n(trace shortened; the server log has all of it)";
  }

  private static String stackTrace(Throwable t, int maxFrames) {
    StringBuilder sb = new StringBuilder();
    Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    for (Throwable current = t; current != null && seen.add(current); ) {
      if (current != t) {
        sb.append("\nCaused by: ");
      }
      sb.append(current.getClass().getName()).append(": ").append(current.getMessage());
      StackTraceElement[] frames = current.getStackTrace();
      int shown = Math.min(frames.length, maxFrames);
      for (int i = 0; i < shown; i++) {
        sb.append("\n\tat ").append(frames[i]);
      }
      if (shown < frames.length) {
        sb.append("\n\t... ").append(frames.length - shown).append(" more");
      }
      current = current.getCause();
    }
    return sb.toString();
  }

  // Depth of the collection being packed. A list that contains itself would otherwise recurse
  // until the stack overflows.
  private static final ThreadLocal<int[]> packDepth = ThreadLocal.withInitial(() -> new int[1]);

  private int[] packNested(FlatBufferBuilder builder, Object value, Set<Long> sessionObjectIds) {
    int[] depth = packDepth.get();
    if (depth[0] >= ResourceLimits.MAX_NESTING_DEPTH) {
      throw new IllegalArgumentException(
          "Result is nested more than "
              + ResourceLimits.MAX_NESTING_DEPTH
              + " levels deep (a collection that contains itself?)");
    }
    depth[0]++;
    try {
      return packValue(builder, value, sessionObjectIds);
    } finally {
      depth[0]--;
    }
  }

  // --- HELPER: Pack a single value and return [type, offset] ---
  private int[] packValue(FlatBufferBuilder builder, Object value, Set<Long> sessionObjectIds) {
    byte type;
    int valueOffset;

    if (value == null) {
      type = Value.NullVal;
      NullVal.startNullVal(builder);
      valueOffset = NullVal.endNullVal(builder);
    } else if (value instanceof Boolean) {
      type = Value.BoolVal;
      valueOffset = BoolVal.createBoolVal(builder, (Boolean) value);
    } else if (value instanceof Character) {
      type = Value.CharVal;
      valueOffset = CharVal.createCharVal(builder, (Character) value);
    } else if (value instanceof Integer
        || value instanceof Long
        || value instanceof Short
        || value instanceof Byte) {
      type = Value.IntVal;
      valueOffset = IntVal.createIntVal(builder, ((Number) value).longValue());
    } else if (value instanceof Double || value instanceof Float) {
      type = Value.DoubleVal;
      valueOffset = DoubleVal.createDoubleVal(builder, ((Number) value).doubleValue());
    } else if (value instanceof String) {
      type = Value.StringVal;
      int sOff = builder.createString((String) value);
      valueOffset = StringVal.createStringVal(builder, sOff);
    } else if (value instanceof ObjectRefT) {
      type = Value.ObjectRef;
      valueOffset = ObjectRef.createObjectRef(builder, ((ObjectRefT) value).id);
    } else if (value instanceof List<?> list) {
      type = Value.ListVal;
      valueOffset = packList(builder, list, sessionObjectIds);
    } else if (value instanceof Map<?, ?> map) {
      type = Value.MapVal;
      List<Object> keys = new ArrayList<>(map.size());
      List<Object> values = new ArrayList<>(map.size());
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        keys.add(entry.getKey());
        values.add(entry.getValue());
      }
      int keysOffset = packList(builder, keys, sessionObjectIds);
      int valuesOffset = packList(builder, values, sessionObjectIds);
      valueOffset = MapVal.createMapVal(builder, keysOffset, valuesOffset);
    } else if (value.getClass().isArray()) {
      // Handle Java arrays
      type = Value.ArrayVal;
      valueOffset = packArray(builder, value, sessionObjectIds);
    } else if (value.getClass() == java.math.BigInteger.class) {
      // The exact class, here and below: a subclass may carry more than the number, and
      // toString() of one need not be the number at all
      type = Value.BigIntVal;
      int bytes = BigIntVal.createVVector(builder, ((java.math.BigInteger) value).toByteArray());
      valueOffset = BigIntVal.createBigIntVal(builder, bytes);
    } else if (value.getClass() == java.math.BigDecimal.class) {
      type = Value.DecimalVal;
      int text = builder.createString(value.toString());
      valueOffset = DecimalVal.createDecimalVal(builder, text);
    } else {
      // Unknown type - wrap as object reference
      long newId = objectIdCounter.getAndIncrement();
      putObject(newId, value);
      if (sessionObjectIds != null) {
        sessionObjectIds.add(newId);
        List<Long> recorder = packRecorder.get();
        if (recorder != null) {
          recorder.add(newId);
        }
      }
      type = Value.ObjectRef;
      valueOffset = ObjectRef.createObjectRef(builder, newId);
    }

    return new int[] {type, valueOffset};
  }

  // What every element of a list is, for packList
  private static final int MIXED = 0;
  private static final int INTEGERS = 1;
  private static final int FLOATS = 2;
  private static final int BOOLEANS = 3;
  private static final int STRINGS = 4;

  private static int elementKind(Object element) {
    if (element instanceof String) {
      return STRINGS;
    } else if (element instanceof Integer
        || element instanceof Long
        || element instanceof Short
        || element instanceof Byte) {
      return INTEGERS;
    } else if (element instanceof Double || element instanceof Float) {
      return FLOATS;
    } else if (element instanceof Boolean) {
      return BOOLEANS;
    }
    return MIXED;
  }

  /** Pack a list as a ListVal: one vector if its elements are of one simple kind. */
  private int packList(FlatBufferBuilder builder, List<?> list, Set<Long> sessionObjectIds) {
    // Copied first: the list may be one that other threads change, and its size is used twice
    Object[] elements = list.toArray();
    int n = elements.length;
    int kind = n == 0 ? MIXED : elementKind(elements[0]);
    for (int i = 1; i < n && kind != MIXED; i++) {
      if (elementKind(elements[i]) != kind) {
        kind = MIXED;
      }
    }
    switch (kind) {
      case INTEGERS -> {
        long[] values = new long[n];
        for (int i = 0; i < n; i++) {
          values[i] = ((Number) elements[i]).longValue();
        }
        int vector = ListVal.createLongsVector(builder, values);
        ListVal.startListVal(builder);
        ListVal.addLongs(builder, vector);
      }
      case FLOATS -> {
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
          values[i] = ((Number) elements[i]).doubleValue();
        }
        int vector = ListVal.createDoublesVector(builder, values);
        ListVal.startListVal(builder);
        ListVal.addDoubles(builder, vector);
      }
      case BOOLEANS -> {
        boolean[] values = new boolean[n];
        for (int i = 0; i < n; i++) {
          values[i] = (Boolean) elements[i];
        }
        int vector = ListVal.createBoolsVector(builder, values);
        ListVal.startListVal(builder);
        ListVal.addBools(builder, vector);
      }
      case STRINGS -> {
        int[] lengths = new int[n];
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        for (int i = 0; i < n; i++) {
          byte[] encoded = ((String) elements[i]).getBytes(java.nio.charset.StandardCharsets.UTF_8);
          lengths[i] = encoded.length;
          bytes.writeBytes(encoded);
        }
        int bytesVector = ListVal.createStringBytesVector(builder, bytes.toByteArray());
        int lengthsVector = ListVal.createStringLengthsVector(builder, lengths);
        ListVal.startListVal(builder);
        ListVal.addStringLengths(builder, lengthsVector);
        ListVal.addStringBytes(builder, bytesVector);
      }
      default -> {
        int[] itemOffsets = new int[n];
        for (int i = 0; i < n; i++) {
          int[] packed = packNested(builder, elements[i], sessionObjectIds);
          Argument.startArgument(builder);
          Argument.addValType(builder, (byte) packed[0]);
          Argument.addVal(builder, packed[1]);
          itemOffsets[i] = Argument.endArgument(builder);
        }
        int vector = ListVal.createItemsVector(builder, itemOffsets);
        ListVal.startListVal(builder);
        ListVal.addItems(builder, vector);
      }
    }
    return ListVal.endListVal(builder);
  }

  // --- HELPER: Pack Java array into ArrayVal ---
  private int packArray(FlatBufferBuilder builder, Object array, Set<Long> sessionObjectIds) {
    Class<?> componentType = array.getClass().getComponentType();

    if (componentType == int.class) {
      int[] arr = (int[]) array;
      int vecOffset = ArrayVal.createIntValuesVector(builder, arr);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Int);
      ArrayVal.addIntValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == long.class) {
      long[] arr = (long[]) array;
      int vecOffset = ArrayVal.createLongValuesVector(builder, arr);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Long);
      ArrayVal.addLongValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == double.class) {
      double[] arr = (double[]) array;
      int vecOffset = ArrayVal.createDoubleValuesVector(builder, arr);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Double);
      ArrayVal.addDoubleValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == float.class) {
      float[] arr = (float[]) array;
      // Widen float[] to double[]
      double[] widened = new double[arr.length];
      for (int i = 0; i < arr.length; i++) {
        widened[i] = arr[i];
      }
      int vecOffset = ArrayVal.createDoubleValuesVector(builder, widened);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Float);
      ArrayVal.addDoubleValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == boolean.class) {
      boolean[] arr = (boolean[]) array;
      int vecOffset = ArrayVal.createBoolValuesVector(builder, arr);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Bool);
      ArrayVal.addBoolValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == byte.class) {
      byte[] arr = (byte[]) array;
      int vecOffset = ArrayVal.createByteValuesVector(builder, arr);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Byte);
      ArrayVal.addByteValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == short.class) {
      short[] arr = (short[]) array;
      // Widen short[] to int[]
      int[] widened = new int[arr.length];
      for (int i = 0; i < arr.length; i++) {
        widened[i] = arr[i];
      }
      int vecOffset = ArrayVal.createIntValuesVector(builder, widened);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Short);
      ArrayVal.addIntValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == char.class) {
      char[] arr = (char[]) array;
      // Widen char[] to int[]
      int[] widened = new int[arr.length];
      for (int i = 0; i < arr.length; i++) {
        widened[i] = arr[i];
      }
      int vecOffset = ArrayVal.createIntValuesVector(builder, widened);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Char);
      ArrayVal.addIntValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else {
      // Object array (String[], Object[], etc.)
      Object[] arr = (Object[]) array;
      int[] argOffsets = new int[arr.length];
      for (int i = 0; i < arr.length; i++) {
        int[] packed = packValue(builder, arr[i], sessionObjectIds);
        Argument.startArgument(builder);
        Argument.addValType(builder, (byte) packed[0]);
        Argument.addVal(builder, packed[1]);
        argOffsets[i] = Argument.endArgument(builder);
      }
      int vecOffset = ArrayVal.createObjectValuesVector(builder, argOffsets);
      ArrayVal.startArrayVal(builder);
      // Use String type for String[], Object for everything else
      if (componentType == String.class) {
        ArrayVal.addElementType(builder, ElementType.String);
      } else {
        ArrayVal.addElementType(builder, ElementType.Object);
      }
      ArrayVal.addObjectValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    }
  }

  // --- CALLBACK HELPERS ---

  /** The method names a RegisterCallback command lists: those of a handler that is an object. */
  private static Set<String> ownMethods(Command cmd) {
    List<String> names = new ArrayList<>();
    for (int i = 0; i < cmd.callbackMethodsLength(); i++) {
      names.add(cmd.callbackMethods(i));
    }
    return Set.copyOf(names);
  }

  /**
   * Creates a dynamic proxy that implements the specified interface. When any method on the proxy
   * is called, it sends a callback request to Python and waits for the response.
   *
   * <p>The SessionContext is captured at proxy creation time: the proxy can be called by any
   * thread, long after the request that registered it, and has to find its session.
   *
   * @param ownMethods the method names of a Python handler that is an object; empty for a function
   */
  private Object createCallbackProxy(
      long callbackId, String interfaceName, Set<String> ownMethods, SessionContext ctx)
      throws ClassNotFoundException {
    Class<?> interfaceClass = getClass(interfaceName);
    if (!interfaceClass.isInterface()) {
      throw new IllegalArgumentException(interfaceName + " is not an interface");
    }

    InvocationHandler handler =
        (proxy, method, args) -> {
          // Handle Object methods locally
          if (method.getDeclaringClass() == Object.class) {
            String methodName = method.getName();
            if ("toString".equals(methodName)) {
              return "PythonCallback[" + callbackId + "]@" + interfaceName;
            } else if ("hashCode".equals(methodName)) {
              return (int) callbackId;
            } else if ("equals".equals(methodName)) {
              return proxy == args[0];
            }
          }

          // A default method runs as Java wrote it and reaches Python through the abstract
          // method it calls: comparator.reversed() calls compare(), not Python's "reversed".
          // Unless the handler is an object with a method of that name: it overrides the
          // default, as a class implementing the interface would (a listener whose methods
          // all have empty default bodies is implemented in no other way).
          if (method.isDefault() && !ownMethods.contains(method.getName())) {
            return InvocationHandler.invokeDefault(proxy, method, args);
          }

          try {
            Object result =
                invokeCallback(
                    callbackId, method.getName(), args == null ? new Object[0] : args, ctx);
            // A cancel that arrived during the round trip, when it could not interrupt. It is
            // aimed at the request's thread; another thread's call carries on.
            if (ctx.control.isSessionThread() && ctx.control.isCancelled()) {
              throw new InterruptedException("Request was cancelled");
            }
            return callbackResult(result, method);
          } finally {
            // The caller may hold the proxy for this call only, and nothing above uses it: without
            // this the collector could take it, and unregister the callback, with the call under
            // way (see proxyCollected)
            Reference.reachabilityFence(proxy);
          }
        };

    return Proxy.newProxyInstance(
        interfaceClass.getClassLoader(),
        new Class<?>[] {interfaceClass},
        new CallbackHandler(callbackId, handler));
  }

  /**
   * Sends a callback invocation request to Python and blocks until Python responds. This is called
   * from the dynamic proxy when Java code invokes a method on the callback object.
   *
   * <p>The thread running the session's request calls Python in band: Python is reading the
   * session's channel for that request's answer, and the session's shared memory is confined to
   * that thread. Every other thread (a parallel stream's worker, a Spark listener, an executor, a
   * timer), at any time, uses the session's callback connection.
   */
  private Object invokeCallback(
      long callbackId, String methodName, Object[] args, SessionContext ctx) throws Exception {
    if (ctx == null || ctx.channel == null || ctx.sharedMem == null) {
      throw new IllegalStateException("Callback invoked with invalid session context");
    }
    boolean inBand = ctx.control.isSessionThread();
    if (ctx.closed) {
      throw new IllegalStateException(
          "Python callback " + methodName + " was called after its client disconnected");
    }
    // Before anything is packed: the objects among the arguments would be registered for a
    // client that will not take them
    if (!callbackRegistry.containsKey(callbackId)) {
      throw new IllegalStateException(
          "Python callback " + methodName + " was called after it was unregistered");
    }

    GatunEvents.CallbackEvent event =
        GatunEvents.beginCallback(ctx.quota.getSessionId(), callbackId, methodName, args.length);
    boolean success = false;
    String errorMessage = null;
    try {
      Object returnValue =
          inBand
              ? exchangeCallback(callbackId, methodName, args, ctx)
              : callOnChannel(callbackId, methodName, args, ctx);
      success = true;
      return returnValue;
    } catch (Exception e) {
      errorMessage = e.getMessage();
      throw e;
    } finally {
      Metrics.getInstance().recordCallback(success);
      GatunEvents.endCallback(event, success, errorMessage);
    }
  }

  /**
   * Check and convert what Python returned for a callback against the Java method's return type, so
   * a mismatch is reported as one instead of surfacing as a ClassCastException or
   * NullPointerException somewhere inside the caller.
   */
  private static Object callbackResult(Object result, java.lang.reflect.Method method) {
    Class<?> returnType = method.getReturnType();
    if (returnType == void.class) {
      return null;
    }
    if (result == null) {
      if (returnType.isPrimitive()) {
        throw new IllegalStateException(
            "Python callback returned None for "
                + method.getName()
                + ", which returns "
                + returnType.getName());
      }
      return null;
    }
    Object converted = MethodResolver.coerce(result, returnType);
    if (!MethodResolver.boxedType(returnType).isInstance(converted)) {
      throw new IllegalStateException(
          "Python callback returned a "
              + result.getClass().getName()
              + " for "
              + method.getName()
              + ", which returns "
              + returnType.getName());
    }
    return converted;
  }

  /**
   * Build the request that asks Python to run a callback. The objects among the arguments are
   * registered for the session while packing and counted, because the client frees them like any
   * other object. Their IDs are added to {@code registered}, for a caller that finds it cannot
   * deliver the request; if the request cannot be built they are dropped here.
   *
   * @param callId the call's ID on the callback connection; 0 in band
   */
  private ByteBuffer buildCallbackRequest(
      long callId,
      long callbackId,
      String methodName,
      Object[] args,
      SessionContext ctx,
      List<Long> registered) {
    FlatBufferBuilder builder = new FlatBufferBuilder(1024);
    List<Long> outer = packRecorder.get();
    packRecorder.set(registered);
    boolean built = false;
    try {
      int[] argOffsets = new int[args.length];
      for (int i = 0; i < args.length; i++) {
        int[] packed = packValue(builder, args[i], ctx.sessionObjectIds);
        Argument.startArgument(builder);
        Argument.addValType(builder, (byte) packed[0]);
        Argument.addVal(builder, packed[1]);
        argOffsets[i] = Argument.endArgument(builder);
      }

      int argsVec = Response.createCallbackArgsVector(builder, argOffsets);
      int methodOff = builder.createString(methodName);

      Response.startResponse(builder);
      Response.addIsError(builder, false);
      Response.addIsCallback(builder, true);
      Response.addCallbackId(builder, callbackId);
      Response.addCallbackMethod(builder, methodOff);
      Response.addCallbackArgs(builder, argsVec);
      Response.addCallId(builder, callId);
      builder.finish(Response.endResponse(builder));

      ByteBuffer request = builder.dataBuffer();
      // The response zone in band, a frame on the callback connection: the same size
      if (request.remaining() > RESPONSE_ZONE_SIZE) {
        throw new IllegalArgumentException(
            "Arguments for callback "
                + methodName
                + " take "
                + request.remaining()
                + " bytes, more than the "
                + RESPONSE_ZONE_SIZE
                + " a callback request can carry");
      }
      built = true;
      return request;
    } finally {
      packRecorder.set(outer);
      ctx.quota.countRegisteredObjects(registered.size());
      if (!built) {
        discardObjects(registered, ctx.sessionObjectIds, ctx.quota);
        registered.clear();
      }
    }
  }

  /** What Python answered: the callback's result, or its failure as an exception. */
  private Object readCallbackResult(Command cmd) {
    if (cmd.action() != Action.CallbackResponse) {
      throw new RuntimeException("Expected CallbackResponse but got action: " + cmd.action());
    }

    Object returnValue = null;
    if (cmd.argsLength() > 0) {
      returnValue = convertArgument(cmd.args(0))[0];
    }

    // The second argument, if present, says whether the callback failed; the message is then
    // in target_name
    boolean isError = false;
    if (cmd.argsLength() > 1) {
      Object[] converted = convertArgument(cmd.args(1));
      if (converted[0] instanceof Boolean) {
        isError = (Boolean) converted[0];
      }
    }
    if (isError) {
      throw new RuntimeException("Callback error: " + cmd.targetName());
    }
    return returnValue;
  }

  /** One callback round-trip in band: the request thread sends and reads on its own channel. */
  private Object exchangeCallback(
      long callbackId, String methodName, Object[] args, SessionContext ctx) throws Exception {

    SocketChannel channel = ctx.channel;
    MemorySegment sharedMem = ctx.sharedMem;

    // The round trip uses the socket, so a cancel must not interrupt it
    ctx.control.beginSocketUse();
    try {
      ByteBuffer resBuf =
          buildCallbackRequest(0, callbackId, methodName, args, ctx, new ArrayList<>());
      int resSize = resBuf.remaining();

      // Write to response zone
      MemorySegment responseSlice = sharedMem.asSlice(this.responseOffset, resSize);
      responseSlice.copyFrom(MemorySegment.ofBuffer(resBuf));

      // Signal Python with the response size
      ByteBuffer lengthBuf = ByteBuffer.allocate(4);
      lengthBuf.order(ByteOrder.LITTLE_ENDIAN);
      lengthBuf.putInt(resSize);
      lengthBuf.flip();
      while (lengthBuf.hasRemaining()) {
        channel.write(lengthBuf);
      }

      // Now we need to read the callback response directly from Python
      // Python sends a CallbackResponse command back to us
      lengthBuf.clear();
      if (!readFully(channel, lengthBuf)) {
        throw new IOException("Connection closed while waiting for callback response");
      }
      lengthBuf.flip();
      int commandSize = lengthBuf.getInt();

      // Validate command size
      if (commandSize <= 0 || commandSize > PAYLOAD_OFFSET) {
        throw new IOException("Invalid callback response size: " + commandSize);
      }

      // Read the command from command zone
      MemorySegment cmdSlice = sharedMem.asSlice(COMMAND_OFFSET, commandSize);
      ByteBuffer cmdBuf = cmdSlice.asByteBuffer();
      cmdBuf.order(ByteOrder.LITTLE_ENDIAN);

      return readCallbackResult(Command.getRootAsCommand(cmdBuf));
    } finally {
      ctx.control.endSocketUse();
    }
  }

  /**
   * One callback round-trip on the session's callback connection, for a thread that is not running
   * the session's request.
   */
  private Object callOnChannel(
      long callbackId, String methodName, Object[] args, SessionContext ctx) {
    CallbackChannel channel = ctx.callbackChannel;
    if (channel == null || channel.isClosed()) {
      throw new IllegalStateException(
          "Python callback "
              + methodName
              + " was called from thread "
              + Thread.currentThread().getName()
              + ", which is not the thread running its client's request, and the client's"
              + " callback connection is "
              + (channel == null ? "not open" : "closed"));
    }

    // Packing the arguments and reading the result register and look up the session's objects.
    // This thread may have no session, in which case ownership would go unchecked, or be
    // running another session's request.
    SessionContext previous = sessionContext.get();
    sessionContext.set(ctx);
    long callId = channel.begin();
    CallMemory memory = new CallMemory(ctx.quota.getSessionId(), callId);
    // Until Python answers, the session is in use: the idle reaper must leave it alone
    ctx.channelCalls.incrementAndGet();
    ctx.quota.touchActivity();
    List<Long> registered = new ArrayList<>();
    boolean sent = false;
    try {
      ByteBuffer request =
          buildCallbackRequest(callId, callbackId, methodName, args, ctx, registered);
      if (ctx.closed) {
        // The session ended while the arguments were being packed, and its cleanup may
        // already have gone past the objects registered just now
        throw new IOException("its client disconnected");
      }
      channel.send(request);
      sent = true;
      while (true) {
        Object item = channel.await(callId);
        if (item instanceof CallbackAnswer answer) {
          if (answer.failure() != null) {
            throw answer.failure();
          }
          return answer.value();
        }
        Command frame = (Command) item;
        if (frame.action() != Action.NestedCommand) {
          throw new IllegalStateException(
              "Unexpected action "
                  + frame.action()
                  + " while Python callback "
                  + methodName
                  + " was running");
        }
        // The callback is calling Java. This thread is the one waiting for the callback, so
        // it runs the call: the session's own thread may be busy with a request, possibly
        // the very request that is waiting for this thread.
        channel.send(runNested(frame, callId, ctx, memory));
      }
    } catch (IOException e) {
      // Nobody will free the arguments' objects if the request never went out, or if the
      // session is over. Otherwise Python may hold them: it frees them, or the session's end
      // does.
      if (!sent || ctx.closed) {
        discardObjects(registered, ctx.sessionObjectIds, ctx.quota);
      }
      // Unchecked: the interface method the caller invoked declares no such exception, and a
      // proxy wraps what its method does not declare in one without a message
      throw new IllegalStateException(
          "Python callback " + methodName + " could not be called: " + e.getMessage(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while Python callback " + methodName + " was running", e);
    } finally {
      channel.end(callId);
      memory.close();
      ctx.channelCalls.decrementAndGet();
      ctx.quota.touchActivity();
      if (previous == null) {
        sessionContext.remove();
      } else {
        sessionContext.set(previous);
      }
    }
  }

  /**
   * Run a command that a callback sent on the callback connection, and build its response.
   *
   * <p>Runs on the thread that called the callback, never the session's own, so nothing here may
   * touch the session's shared memory: it is confined to the session's thread. Arrow tables go
   * through the call's own memory instead.
   */
  private ByteBuffer runNested(
      Command envelope, long callId, SessionContext ctx, CallMemory memory) {
    FlatBufferBuilder builder = new FlatBufferBuilder(256);
    List<Long> registered = new ArrayList<>();
    // What a vectorized command has made so far, should a later step of it fail
    List<Object> vector = new ArrayList<>();
    RequestContext request = null;
    Object result = null;
    String errorType = null;
    int response;
    try {
      ByteBuffer serialized = envelope.nestedCommandAsByteBuffer();
      if (serialized == null) {
        throw new IllegalArgumentException("NestedCommand carries no command");
      }
      Command cmd = Command.getRootAsCommand(serialized);
      request =
          RequestContext.create(
              ctx.quota.getSessionId(), cmd.requestId(), cmd.action(), cmd.targetName());
      if (ctx.closed) {
        throw new IllegalStateException("The client has disconnected");
      }
      if (cmd.action() == Action.Batch) {
        response =
            runBatch(
                cmd.batch(),
                builder,
                ctx.sessionObjectIds,
                ctx.sessionCallbackIds,
                ctx.quota,
                registered,
                callId);
      } else {
        result = executeNested(cmd, ctx, memory, vector);
        response =
            result instanceof ArrowStreamLocation stream
                ? packArrowStreamResponse(builder, stream.offset(), stream.length(), callId)
                : packResult(builder, result, ctx.sessionObjectIds, ctx.quota, registered, callId);
      }
    } catch (Throwable t) {
      Throwable cause = unwrap(t);
      if (cause instanceof InterruptedException) {
        // The interrupt was meant for this thread, which the server does not own, and not
        // for the command: Python is told, and the wait for the callback then ends
        Thread.currentThread().interrupt();
      }
      LOG.log(Level.FINE, "Command from inside a callback failed: " + cause);
      // The client gets the error, not the results gathered before it
      discardResultRefs(vector, ctx.sessionObjectIds, ctx.quota);
      vector.clear();
      result = null;
      errorType = cause.getClass().getName();
      builder = new FlatBufferBuilder(256);
      response = packError(builder, formatException(cause), errorType, callId);
    }
    builder.finish(response);
    ByteBuffer frame = builder.dataBuffer();
    if (frame.remaining() > CallbackChannel.MAX_FRAME_SIZE) {
      // The client never sees the IDs in the response it was meant to get
      discardNested(result, vector, registered, ctx);
      PayloadTooLargeException tooLarge =
          new PayloadTooLargeException(
              "Response", frame.remaining(), CallbackChannel.MAX_FRAME_SIZE);
      errorType = tooLarge.getClass().getName();
      builder = new FlatBufferBuilder(256);
      builder.finish(packError(builder, tooLarge.getMessage(), errorType, callId));
      frame = builder.dataBuffer();
    } else if (ctx.closed) {
      // The session ended while the command ran, and its cleanup may already have gone past
      // what the command registered
      discardNested(result, vector, registered, ctx);
    }
    if (request != null) {
      // Not through the session's observer: it follows the one request its own thread runs
      StructuredLogger.logRequest(request, errorType == null, errorType);
      Metrics.getInstance()
          .recordRequest(request.action(), request.elapsedMicros(), errorType == null, errorType);
    }
    return frame;
  }

  /**
   * Drop the objects a nested command registered for a client that will not receive them.
   *
   * @param vector what a vectorized command made. Not taken from {@code result}: a list there may
   *     as well be one of the user's own that a method returned, and walking that runs their code
   */
  private void discardNested(
      Object result, List<Object> vector, List<Long> registered, SessionContext ctx) {
    if (result instanceof ObjectRefT ref) {
      // If it is a callback's proxy, the callback goes with it
      unregisterCallback(ref.id, ctx.sessionCallbackIds);
      freeObject(ref.id, ctx.sessionObjectIds, ctx.quota);
    }
    discardResultRefs(vector, ctx.sessionObjectIds, ctx.quota);
    discardObjects(registered, ctx.sessionObjectIds, ctx.quota);
  }

  /**
   * Read several fields of one object. The results go into {@code results} as they are made, so
   * that the caller can release the objects among them if a later step fails.
   */
  private void getFields(
      Command cmd, Set<Long> sessionObjectIds, SessionQuota quota, List<Object> results)
      throws Throwable {
    // Vectorized field reads: get multiple fields from one object
    long targetId = cmd.targetId();
    Object target = sessionObject(targetId);
    if (target == null) {
      throw new RuntimeException("Object " + targetId + " not found");
    }

    GetFieldsRequest req = cmd.getFields();
    if (req == null) {
      throw new IllegalArgumentException("GetFields action requires get_fields field");
    }

    int fieldCount = req.fieldNamesLength();

    for (int i = 0; i < fieldCount; i++) {
      String fieldName = req.fieldNames(i);
      allowlist.checkInstanceMember(target, fieldName);

      // Try VarHandle for faster access
      java.lang.invoke.VarHandle vh = getInstanceVarHandle(target.getClass(), fieldName);
      Object fieldValue;
      if (vh != null) {
        fieldValue = vh.get(target);
      } else {
        java.lang.reflect.Field field = getInstanceField(target.getClass(), fieldName);
        field.setAccessible(true);
        fieldValue = field.get(target);
      }

      // Wrap non-auto-convertible objects
      if (fieldValue != null && !MethodResolver.isAutoConvertible(fieldValue)) {
        quota.allocateObject();
        long newId = objectIdCounter.getAndIncrement();
        putObject(newId, fieldValue);
        sessionObjectIds.add(newId);
        fieldValue = new ObjectRefT(newId);
      }
      results.add(fieldValue);
    }
  }

  /**
   * Call several methods of one object, in order. The results go into {@code results} as they are
   * made, so that the caller can release the objects among them if a later step fails.
   */
  private void invokeMethods(
      Command cmd, Set<Long> sessionObjectIds, SessionQuota quota, List<Object> results)
      throws Throwable {
    // Vectorized method calls: invoke multiple methods on same target
    long targetId = cmd.targetId();
    Object target = sessionObject(targetId);
    if (target == null) {
      throw new RuntimeException("Object " + targetId + " not found");
    }

    InvokeMethodsRequest req = cmd.invokeMethods();
    if (req == null) {
      throw new IllegalArgumentException("InvokeMethods action requires invoke_methods field");
    }

    int callCount = req.methodCallsLength();
    ResourceLimits.checkMethodCallsSize(callCount);

    for (int i = 0; i < callCount; i++) {
      MethodCall call = req.methodCalls(i);
      String methodName = call.methodName();
      allowlist.checkInstanceMember(target, methodName);
      boolean returnObjectRef = call.returnObjectRef();

      // Convert arguments
      int argCount = call.argsLength();
      Object[] javaArgs = new Object[argCount];
      Class<?>[] argTypes = new Class<?>[argCount];
      for (int j = 0; j < argCount; j++) {
        Argument arg = call.args(j);
        Object[] converted = convertArgument(arg);
        javaArgs[j] = converted[0];
        argTypes[j] = (Class<?>) converted[1];
      }

      // Invoke method
      Object methodResult;
      if (argCount == 0) {
        MethodHandle handle = getNoArgMethodHandle(target.getClass(), methodName);
        if (handle != null) {
          methodResult = handle.invoke(target);
        } else {
          var mwa =
              MethodResolver.findMethodWithArgs(target.getClass(), methodName, argTypes, javaArgs);
          methodResult = mwa.cached.invoke(target, mwa.args);
        }
      } else {
        var mwa =
            MethodResolver.findMethodWithArgs(target.getClass(), methodName, argTypes, javaArgs);
        methodResult = mwa.cached.invoke(target, mwa.args);
      }

      // Wrap result if needed
      if (methodResult != null
          && (returnObjectRef || !MethodResolver.isAutoConvertible(methodResult))) {
        quota.allocateObject();
        long newId = objectIdCounter.getAndIncrement();
        putObject(newId, methodResult);
        sessionObjectIds.add(newId);
        methodResult = new ObjectRefT(newId);
      }
      results.add(methodResult);
    }
  }

  /**
   * Create several objects. The results go into {@code results} as they are made, so that the
   * caller can release the objects among them if a later step fails.
   */
  private void createObjects(
      Command cmd, Set<Long> sessionObjectIds, SessionQuota quota, List<Object> results)
      throws Throwable {
    // Vectorized object creation: create multiple objects
    CreateObjectsRequest req = cmd.createObjects();
    if (req == null) {
      throw new IllegalArgumentException("CreateObjects action requires create_objects field");
    }

    int objCount = req.objectsLength();
    ResourceLimits.checkCreateObjectsSize(objCount);

    for (int i = 0; i < objCount; i++) {
      ObjectSpec spec = req.objects(i);
      String className = spec.className();

      requireClassAllowed(className);

      // Refuse before running the constructor; the slot is taken once it succeeds
      quota.checkObjectLimit();

      Class<?> clazz = getClass(className);
      Object instance;

      int argCount = spec.argsLength();
      if (argCount == 0) {
        instance = getNoArgConstructor(clazz).newInstance();
      } else {
        Object[] javaArgs = new Object[argCount];
        Class<?>[] argTypes = new Class<?>[argCount];
        for (int j = 0; j < argCount; j++) {
          Argument arg = spec.args(j);
          Object[] converted = convertArgument(arg);
          javaArgs[j] = converted[0];
          argTypes[j] = (Class<?>) converted[1];
        }

        var ctorResult = MethodResolver.findConstructorWithArgs(clazz, argTypes, javaArgs);
        instance = ctorResult.cached.newInstance(ctorResult.args);
      }

      quota.allocateObject();
      long newId = objectIdCounter.getAndIncrement();
      putObject(newId, instance);
      sessionObjectIds.add(newId);
      results.add(new ObjectRefT(newId));
    }
  }

  /**
   * Run a batch's sub-commands in order and pack the response, one sub-response each, into {@code
   * builder}. A sub-command that fails gets an error sub-response; the batch itself does not fail,
   * unless the thread was interrupted: see below.
   *
   * @param registered receives the IDs of the objects the batch registered for the client, those
   *     its commands made and those registered while packing results, for a caller that cannot
   *     deliver the response
   * @param callId the call the response belongs to on the callback connection, or 0
   */
  private int runBatch(
      BatchCommand batch,
      FlatBufferBuilder builder,
      Set<Long> sessionObjectIds,
      Set<Long> sessionCallbackIds,
      SessionQuota quota,
      List<Long> registered,
      long callId)
      throws InterruptedException {
    if (batch == null) {
      throw new IllegalArgumentException("Batch action requires batch field");
    }

    int commandCount = batch.commandsLength();
    ResourceLimits.checkBatchSize(commandCount);
    boolean stopOnError = batch.stopOnError();

    int[] subResponseOffsets = new int[commandCount];
    int errorIndex = -1;
    int executedCount = 0;

    for (int i = 0; i < commandCount; i++) {
      Command subCmd = batch.commands(i);
      try {
        // None of the commands a batch may hold uses shared memory
        Object subResult =
            executeCommand(subCmd, sessionObjectIds, sessionCallbackIds, null, quota);
        if (subResult instanceof ObjectRefT made) {
          registered.add(made.id);
        }
        subResponseOffsets[i] = packResult(builder, subResult, sessionObjectIds, quota, registered);
        executedCount++;
      } catch (Throwable subT) {
        Throwable subCause = unwrap(subT);
        if (subCause instanceof InterruptedException interrupted) {
          // The request was cancelled or, on the callback connection, the thread running it
          // was interrupted. That is not this one command's failure: nothing after it may run,
          // whatever stop_on_error says, and the client gets the cancellation in place of
          // results, some of which would stand for commands that never ran.
          discardObjects(registered, sessionObjectIds, quota);
          registered.clear();
          throw interrupted;
        }
        subResponseOffsets[i] =
            packError(builder, formatException(subCause), subCause.getClass().getName());
        executedCount++;

        if (stopOnError) {
          errorIndex = i;
          break; // The remaining commands are not run
        }
      }
    }
    return packBatchResponse(builder, subResponseOffsets, executedCount, errorIndex, callId);
  }

  /**
   * A callback's proxy has been garbage collected: the client released its handle on it and no Java
   * code holds it any more, so nothing can call it again. The callback is unregistered and the
   * client is told, so that it can drop the Python function. Runs on the cleaner's thread.
   */
  private void proxyCollected(long callbackId, SessionContext ctx) {
    if (!ctx.sessionCallbackIds.remove(callbackId)) {
      return; // unregistered already
    }
    callbackRegistry.remove(callbackId);
    LOG.fine("Unregistered callback ID " + callbackId + ": its proxy was garbage collected");
    CallbackChannel channel = ctx.callbackChannel;
    if (ctx.closed || channel == null) {
      return;
    }
    FlatBufferBuilder builder = new FlatBufferBuilder(64);
    Response.startResponse(builder);
    Response.addCallbackId(builder, callbackId);
    Response.addCallbackReleased(builder, true);
    builder.finish(Response.endResponse(builder));
    try {
      channel.send(builder.dataBuffer());
    } catch (IOException e) {
      // The client is gone, and its callbacks with it
    }
  }

  /**
   * Make a proxy for a Python callback and register it, as callback and as object, for {@code ctx}.
   */
  private ObjectRefT registerCallback(Command cmd, SessionContext ctx) throws Exception {
    String interfaceName = cmd.targetName();
    // Refuse before creating the proxy; the slot is taken once that succeeds
    ctx.quota.checkObjectLimit();

    // One ID for the callback and for the proxy as an object: Python uses the object's ID as
    // the callback's
    long callbackId = objectIdCounter.getAndIncrement();

    // The proxy keeps the session's context: whichever thread calls it reaches that session
    Object proxy = createCallbackProxy(callbackId, interfaceName, ownMethods(cmd), ctx);
    ctx.quota.allocateObject();

    callbackRegistry.put(callbackId, new CallbackInfo(callbackId, interfaceName, ctx));
    ctx.sessionCallbackIds.add(callbackId);

    putObject(callbackId, proxy);
    ctx.sessionObjectIds.add(callbackId);
    // The action must not refer to the proxy, or it would never run
    PROXY_CLEANER.register(proxy, () -> proxyCollected(callbackId, ctx));

    LOG.fine("Registered callback ID " + callbackId + " for interface " + interfaceName);
    return new ObjectRefT(callbackId);
  }

  /**
   * The commands a callback may send on the callback connection. {@code Batch} is one of them too,
   * but packs its own response: see {@code runNested}.
   */
  private Object executeNested(
      Command cmd, SessionContext ctx, CallMemory memory, List<Object> vector) throws Throwable {
    switch (cmd.action()) {
      case Action.Ping:
        return true;
      case Action.Reflect:
        return reflect(cmd.targetName());
      case Action.CreateObject:
      case Action.FreeObject:
      case Action.InvokeMethod:
      case Action.InvokeStaticMethod:
      case Action.GetField:
      case Action.SetField:
      case Action.IsInstanceOf:
      case Action.GetStaticField:
      case Action.SetStaticField:
        // No shared memory: none of these use it, and this thread could not
        return executeCommand(cmd, ctx.sessionObjectIds, ctx.sessionCallbackIds, null, ctx.quota);
      case Action.UnregisterCallback:
        long unregistered = unregisterCallback(cmd.targetId(), ctx.sessionCallbackIds);
        return unregistered == 0 ? null : unregistered;
      case Action.RegisterCallback:
        return registerCallback(cmd, ctx);
      case Action.GetFields:
        getFields(cmd, ctx.sessionObjectIds, ctx.quota, vector);
        return vector;
      case Action.InvokeMethods:
        invokeMethods(cmd, ctx.sessionObjectIds, ctx.quota, vector);
        return vector;
      case Action.CreateObjects:
        createObjects(cmd, ctx.sessionObjectIds, ctx.quota, vector);
        return vector;
      case Action.GetMetrics:
        return Metrics.getInstance().report();
      case Action.OpenCallMemory:
        return memory.open();
      case Action.ArrowToObject:
        // No loans: a call's memory is gone when the call ends
        return arrowToObject(
            cmd.arrowStream(),
            cmd.arrowForm(),
            memory.segment(),
            memory.arrow(),
            null,
            ctx.sessionObjectIds,
            ctx.quota);
      case Action.ArrowFromObject:
        // The call moves one table at a time, so each starts at the front
        return new ArrowStreamLocation(
            0,
            arrowFromObject(
                cmd.targetId(), memory.segment(), memory.arrow(), ctx.quota.getSessionId()));
      default:
        throw new UnsupportedOperationException(
            RequestContext.actionName(cmd.action())
                + " cannot be used inside a callback that Java made from another thread");
    }
  }

  /**
   * What a dotted name is: "class" if it loads as a class, "method" or "field" if its last segment
   * is a member of the class named before it, "none" otherwise.
   */
  private String reflect(String fullName) {
    try {
      // A Scala object's class has a trailing $
      Class<?> clazz = tryGetClass(fullName);
      if (clazz == null) {
        clazz = tryGetClass(fullName + "$");
      }
      if (clazz != null) {
        return "class";
      }

      int lastDot = fullName.lastIndexOf('.');
      if (lastDot <= 0) {
        return "none";
      }
      String parentName = fullName.substring(0, lastDot);
      String memberName = fullName.substring(lastDot + 1);
      Class<?> parentClass = tryGetClass(parentName);
      if (parentClass == null) {
        parentClass = tryGetClass(parentName + "$");
      }
      if (parentClass == null) {
        return "none";
      }
      for (java.lang.reflect.Method m : getCachedMethods(parentClass)) {
        if (m.getName().equals(memberName)) {
          return "method";
        }
      }
      try {
        parentClass.getField(memberName);
        return "field";
      } catch (NoSuchFieldException e) {
        return "none";
      }
    } catch (Exception e) {
      return "none";
    }
  }

  // --- HELPER: Pack Result into FlatBuffer ---
  private int packSuccess(FlatBufferBuilder builder, Object result) {
    return packSuccessWithSession(builder, result, null, 0);
  }

  private int packSuccessWithSession(
      FlatBufferBuilder builder, Object result, Set<Long> sessionObjectIds, long callId) {
    int[] packed = packValue(builder, result, sessionObjectIds);

    Response.startResponse(builder);
    Response.addIsError(builder, false);
    Response.addReturnValType(builder, (byte) packed[0]);
    Response.addReturnVal(builder, packed[1]);
    if (callId != 0) {
      Response.addCallId(builder, callId);
    }
    return Response.endResponse(builder);
  }

  private int packError(FlatBufferBuilder builder, String msg, String errorType) {
    return packError(builder, msg, errorType, 0);
  }

  private int packError(FlatBufferBuilder builder, String msg, String errorType, long callId) {
    int errOff = builder.createString(msg == null ? "Unknown Error" : msg);
    int typeOff =
        builder.createString(errorType == null ? "java.lang.RuntimeException" : errorType);
    Response.startResponse(builder);
    Response.addIsError(builder, true);
    Response.addErrorMsg(builder, errOff);
    Response.addErrorType(builder, typeOff);
    if (callId != 0) {
      Response.addCallId(builder, callId);
    }
    return Response.endResponse(builder);
  }

  /**
   * Pack a batch of responses into a BatchResponse.
   *
   * @param builder The FlatBuffer builder
   * @param responseOffsets Array of Response offsets (already built)
   * @param count Number of responses that were actually built
   * @param errorIndex Index of first error (-1 if no errors)
   */
  private int packBatchResponse(
      FlatBufferBuilder builder, int[] responseOffsets, int count, int errorIndex, long callId) {
    // Create vector of response offsets (only include executed ones)
    int[] toInclude =
        count == responseOffsets.length
            ? responseOffsets
            : java.util.Arrays.copyOf(responseOffsets, count);
    int responsesVec = BatchResponse.createResponsesVector(builder, toInclude);

    // Build BatchResponse
    BatchResponse.startBatchResponse(builder);
    BatchResponse.addResponses(builder, responsesVec);
    BatchResponse.addErrorIndex(builder, errorIndex);
    int batchRespOffset = BatchResponse.endBatchResponse(builder);

    // Wrap in outer Response
    Response.startResponse(builder);
    Response.addIsError(builder, false);
    Response.addBatch(builder, batchRespOffset);
    if (callId != 0) {
      Response.addCallId(builder, callId);
    }
    return Response.endResponse(builder);
  }

  /** Pack an Arrow write result into a response with ArrowBatchDescriptor. */
  /** A response that says where in the payload zone an Arrow IPC stream was written. */
  private int packArrowStreamResponse(
      FlatBufferBuilder builder, long offset, long length, long callId) {
    int stream = BufferDescriptor.createBufferDescriptor(builder, offset, length);
    Response.startResponse(builder);
    Response.addIsError(builder, false);
    Response.addArrowStream(builder, stream);
    if (callId != 0) {
      Response.addCallId(builder, callId);
    }
    return Response.endResponse(builder);
  }

  /**
   * Turn the Arrow IPC stream a client wrote into {@code zone} into a Java object, registered for
   * the session: a table that owns a copy of the data, a table that reads it in place, or the
   * stream's record batches as byte arrays.
   *
   * @param form an {@link ArrowForm}
   * @param zone the memory the stream's offset counts from: the session's payload zone, or a call's
   *     own memory
   * @param arrow reads the stream. A batch it holds in place in the zone must not be overlapped.
   * @param loans the session's lent tables, which must not be overlapped either; null for a call's
   *     memory, where nothing can be lent
   */
  private ObjectRefT arrowToObject(
      BufferDescriptor stream,
      byte form,
      MemorySegment zone,
      ArrowMemoryHandler arrow,
      ArrowLoans loans,
      Set<Long> sessionObjectIds,
      SessionQuota quota)
      throws IOException {
    if (stream == null) {
      throw new IllegalArgumentException("ArrowToObject requires arrow_stream");
    }
    long zoneSize = zone.byteSize();
    long offset = stream.offset();
    long length = stream.length();
    // The values are unsigned on the wire; negative here means larger than any zone
    if (offset < 0 || length <= 0 || offset > zoneSize || length > zoneSize - offset) {
      throw new IllegalArgumentException(
          String.format(
              "Arrow stream is outside the payload zone: offset=%s length=%s, zone is %d bytes",
              Long.toUnsignedString(offset), Long.toUnsignedString(length), zoneSize));
    }
    if (offset < arrow.getLoadedEnd()) {
      throw new IllegalStateException(
          String.format(
              "Arrow stream at offset %d overlaps the batch loaded by send_arrow_buffers,"
                  + " which ends at offset %d. Reset the payload arena first.",
              offset, arrow.getLoadedEnd()));
    }
    if (loans != null && loans.overlaps(offset, offset + length)) {
      throw new IllegalStateException(
          String.format(
              "Arrow stream at offset %d overlaps a table that is lent to Java. End the loan"
                  + " first.",
              offset));
    }

    // Refuse before reading; the slot is taken only once the object exists
    quota.checkObjectLimit();
    Object made;
    long rows;
    long newId = objectIdCounter.getAndIncrement();
    switch (form) {
      case ArrowForm.Table -> {
        quota.holdArrowBytes(length);
        VectorSchemaRoot table;
        try {
          table = arrow.readIpcTable(zone.asSlice(offset, length));
        } catch (Throwable t) {
          quota.releaseHeldArrowBytes(length);
          throw t;
        }
        try {
          quota.allocateObject();
        } catch (RuntimeException e) {
          // Another thread of the session took the last slot since the check above
          quota.releaseHeldArrowBytes(length);
          table.close();
          throw e;
        }
        synchronized (ownedTables) {
          ownedTables.put(table, new OwnedTable(length, quota));
        }
        made = table;
        rows = table.getRowCount();
      }
      case ArrowForm.LentTable -> {
        if (loans == null) {
          throw new UnsupportedOperationException(
              "A table can be lent only on the session's own channel");
        }
        // The data stays in the client's memory and is not charged to max_arrow_bytes; what
        // the table allocates beside it is (ArrowLoans.lend)
        ArrowLoans.Loan loan = loans.lend(offset, length);
        try {
          quota.allocateObject();
        } catch (RuntimeException e) {
          loan.end();
          throw e;
        }
        lentTables.put(newId, loan);
        made = loan.table();
        rows = loan.table().getRowCount();
      }
      case ArrowForm.Batches -> {
        ArrowMemoryHandler.Batches batches =
            ArrowMemoryHandler.readIpcBatches(zone.asSlice(offset, length));
        quota.allocateObject();
        made = batches.batches();
        rows = batches.rows();
      }
      default -> throw new IllegalArgumentException("Unknown arrow_form " + form);
    }
    putObject(newId, made);
    sessionObjectIds.add(newId);
    SessionObserver.recordArrowTransfer(
        quota.getSessionId(), "python_to_java", rows, length, form == ArrowForm.LentTable);
    return new ObjectRefT(newId);
  }

  /** Refuse a batch whose buffers lie on a table the client has lent and not taken back. */
  private static void requireClearOfLoans(ArrowBatchDescriptor batch, ArrowLoans loans) {
    for (int i = 0; i < batch.buffersLength(); i++) {
      BufferDescriptor buffer = batch.buffers(i);
      long offset = buffer.offset();
      long length = buffer.length();
      if (length > 0 && offset >= 0 && loans.overlaps(offset, offset + length)) {
        throw new IllegalStateException(
            "Arrow buffer " + i + " lies on a table that is lent to Java. End the loan first.");
      }
    }
  }

  /**
   * Write the session's table {@code targetId} as an Arrow IPC stream at the start of {@code free}.
   *
   * @return the stream's length
   */
  private long arrowFromObject(
      long targetId, MemorySegment free, ArrowMemoryHandler arrow, long sessionId)
      throws IOException {
    Object target = sessionObject(targetId);
    if (target == null) throw new RuntimeException("Object " + targetId + " not found");
    if (!(target instanceof VectorSchemaRoot table)) {
      throw new IllegalArgumentException(
          "Object "
              + targetId
              + " is a "
              + target.getClass().getName()
              + ", not an org.apache.arrow.vector.VectorSchemaRoot");
    }
    long length = arrow.writeIpcTable(table, free);
    SessionObserver.recordArrowTransfer(
        sessionId, "java_to_python", table.getRowCount(), length, false);
    return length;
  }

  /** Where in a call's memory {@link #arrowFromObject} left a stream for a nested command. */
  private record ArrowStreamLocation(long offset, long length) {}

  // The sizes of the call memories that exist, across sessions (see CallMemory.open)
  private final AtomicLong callMemoryBytes = new AtomicLong();

  /**
   * Shared memory of one call on the callback connection, for the Arrow tables its callback moves.
   *
   * <p>The session's own memory is confined to the session's thread and may be in use by the
   * request that is waiting for this very callback. This one is confined to the thread that waits
   * for the callback, which is the thread that runs the callback's commands, and nothing else
   * shares it: calls in flight at the same time each have their own. It is created by the call's
   * first transfer (most callbacks move no table) and removed when the call ends. The client's
   * mapping of it outlives that, for as long as a table read in place there is alive.
   */
  private final class CallMemory implements AutoCloseable {
    private final Path path;
    private boolean counted;
    private Arena arena;
    private MemorySegment segment;
    private ArrowMemoryHandler arrow;

    CallMemory(long sessionId, long callId) {
      this.path = Path.of(socketPath.toString() + "." + sessionId + ".call" + callId + ".shm");
    }

    /** Create and map the file, unless that is done already. Returns its path for the client. */
    String open() throws IOException {
      if (segment == null) {
        sessionFiles.add(path);
        // Callbacks at once each get a file, every one of them empty when the next checks for
        // free space. Each therefore leaves the others their whole size. That counts what they
        // have written twice, which errs towards refusing a transfer; the other way is a fault
        // when the disk fills up.
        long others = callMemoryBytes.getAndAdd(memorySize);
        counted = true;
        // The mapping does not need the file to stay open
        try (FileChannel file =
            createSharedMemoryFile(path, others, "the shared memory of a callback's tables")) {
          Arena confined = Arena.ofConfined();
          try {
            segment = file.map(FileChannel.MapMode.READ_WRITE, 0, memorySize, confined);
          } catch (IOException | RuntimeException e) {
            confined.close();
            throw e;
          }
          arena = confined;
          arrow = new ArrowMemoryHandler(allocator);
        } catch (IOException | RuntimeException e) {
          // The callback may run for hours yet, and try again: space this call does not use
          // must not stay set aside, least of all against its own next attempt
          release();
          throw e;
        }
      }
      return path.toString();
    }

    MemorySegment segment() {
      if (segment == null) {
        throw new IllegalStateException(
            "The call has no shared memory yet: send OpenCallMemory first");
      }
      return segment;
    }

    ArrowMemoryHandler arrow() {
      return arrow;
    }

    /** Give back the free space set aside for this call's file. */
    private void release() {
      if (counted) {
        callMemoryBytes.addAndGet(-memorySize);
        counted = false;
      }
    }

    /** Must run on the thread that opened it: the mapping is confined to that thread. */
    @Override
    public void close() {
      segment = null;
      if (arena != null) {
        arena.close();
        arena = null;
      }
      // The file goes first: a client that finds it gone writes nothing more there, and only
      // then is its space anyone else's
      if (sessionFiles.remove(path)) {
        try {
          Files.deleteIfExists(path);
        } catch (IOException e) {
          LOG.log(Level.FINE, "Could not remove " + path, e);
        }
      }
      release();
    }
  }

  private int packArrowResponse(
      FlatBufferBuilder builder, ArrowMemoryHandler.ArrowWriteResult result) {
    // Build schema bytes vector if present
    int schemaBytesVec = 0;
    if (result.schemaBytes != null) {
      schemaBytesVec = ArrowBatchDescriptor.createSchemaBytesVector(builder, result.schemaBytes);
    }

    // Build buffer descriptors
    int[] bufferOffsets = new int[result.bufferDescriptors.size()];
    for (int i = 0; i < result.bufferDescriptors.size(); i++) {
      long[] desc = result.bufferDescriptors.get(i);
      BufferDescriptor.startBufferDescriptor(builder);
      BufferDescriptor.addOffset(builder, desc[0]);
      BufferDescriptor.addLength(builder, desc[1]);
      bufferOffsets[i] = BufferDescriptor.endBufferDescriptor(builder);
    }
    int buffersVec = ArrowBatchDescriptor.createBuffersVector(builder, bufferOffsets);

    // Build field nodes
    int[] nodeOffsets = new int[result.fieldNodes.size()];
    for (int i = 0; i < result.fieldNodes.size(); i++) {
      long[] node = result.fieldNodes.get(i);
      FieldNode.startFieldNode(builder);
      FieldNode.addLength(builder, node[0]);
      FieldNode.addNullCount(builder, node[1]);
      nodeOffsets[i] = FieldNode.endFieldNode(builder);
    }
    int nodesVec = ArrowBatchDescriptor.createNodesVector(builder, nodeOffsets);

    // Build ArrowBatchDescriptor
    ArrowBatchDescriptor.startArrowBatchDescriptor(builder);
    ArrowBatchDescriptor.addSchemaHash(builder, result.schemaHash);
    if (schemaBytesVec != 0) {
      ArrowBatchDescriptor.addSchemaBytes(builder, schemaBytesVec);
    }
    ArrowBatchDescriptor.addNumRows(builder, result.numRows);
    ArrowBatchDescriptor.addNodes(builder, nodesVec);
    ArrowBatchDescriptor.addBuffers(builder, buffersVec);
    ArrowBatchDescriptor.addArenaEpoch(builder, result.arenaEpoch);
    int batchDescOffset = ArrowBatchDescriptor.endArrowBatchDescriptor(builder);

    // Build Response with arrow_batch
    Response.startResponse(builder);
    Response.addIsError(builder, false);
    Response.addArrowBatch(builder, batchDescOffset);
    return Response.endResponse(builder);
  }

  static class ObjectRefT {
    public long id;

    public ObjectRefT(long id) {
      this.id = id;
    }
  }

  /**
   * Reads exactly buffer.remaining() bytes from the channel. Returns true if successful, false on
   * EOF.
   */
  private static boolean readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
    buffer.clear();
    while (buffer.hasRemaining()) {
      int read = channel.read(buffer);
      if (read == -1) {
        return false; // EOF
      }
    }
    return true;
  }

  // ========== OBSERVABILITY API ==========

  /**
   * Get the global Metrics instance for monitoring.
   *
   * <p>Use this to access request counts, latency percentiles, object counts, etc. Thread-safe and
   * can be called from any thread.
   */
  public static Metrics getMetrics() {
    return Metrics.getInstance();
  }

  /** Get a formatted metrics report for logging or debugging. */
  public static String getMetricsReport() {
    return Metrics.getInstance().report();
  }

  /**
   * Enable trace mode for verbose method resolution logging.
   *
   * <p>When enabled, logs detailed information about method overload resolution decisions. Useful
   * for debugging "wrong method called" issues.
   *
   * <p>Enable via: -Dgatun.trace=true or call this method.
   */
  public static void setTraceMode(boolean enabled) {
    StructuredLogger.setTraceMode(enabled);
  }

  /** Check if trace mode is enabled. */
  public static boolean isTraceMode() {
    return StructuredLogger.isTraceEnabled();
  }

  public static void main(String[] args) {
    try {
      // Configure logging based on system property
      configureLogging();

      // Log resource limits at startup for debugging
      LOG.info(ResourceLimits.getSummary());

      watchParent(Long.getLong("gatun.parent.pid", 0L));

      long size = 16 * 1024 * 1024;
      String path = System.getProperty("user.home") + "/gatun.sock";
      if (args.length > 0) size = Long.parseLong(args[0]);
      if (args.length > 1) path = args[1];
      new GatunServer(path, size).start();
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "Server failed to start", t);
      System.exit(1);
    }
  }

  /**
   * Exit when the process that launched this server is gone. The launcher stops the server when
   * Python exits normally; this covers a Python process that was killed or crashed, which would
   * otherwise leave the server running for good.
   */
  private static void watchParent(long parentPid) {
    if (parentPid <= 0) {
      return;
    }
    java.util.Optional<ProcessHandle> parent = ProcessHandle.of(parentPid);
    if (parent.isEmpty()) {
      LOG.severe("Parent process " + parentPid + " is not running; exiting");
      System.exit(1);
    }
    parent
        .get()
        .onExit()
        .thenRun(() -> {
          LOG.info("Parent process " + parentPid + " exited; shutting down");
          System.exit(0);
        });
  }

  /**
   * Configure Java logging based on system properties.
   *
   * <p>Reads gatun.log.level to set the logging level for org.gatun.server. Valid levels: FINEST,
   * FINER, FINE, INFO, WARNING, SEVERE
   */
  private static void configureLogging() {
    String levelStr = System.getProperty("gatun.log.level");
    if (levelStr == null) return;

    try {
      Level level = Level.parse(levelStr.toUpperCase());

      // Get the root logger and our logger
      java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");
      java.util.logging.Logger gatunLogger = java.util.logging.Logger.getLogger("org.gatun.server");

      // Set our logger's level
      gatunLogger.setLevel(level);

      // Also set the console handler level (otherwise it filters out FINE/FINER/FINEST)
      for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
        if (handler instanceof java.util.logging.ConsoleHandler) {
          handler.setLevel(level);
        }
      }

      LOG.info("Log level set to: " + level);
    } catch (IllegalArgumentException e) {
      LOG.warning("Invalid log level: " + levelStr + ". Using default.");
    }
  }
}
