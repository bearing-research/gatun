# Threads: one client, a channel per thread

**Status: design. Nothing here is implemented yet.** Today a `GatunClient` shared between threads runs one request at a time (`_exchange_lock` in `client.py`), and a second client has objects of its own. This note says how several threads of one client will call Java at the same time on the same objects, and what that decides for two features that come first: the Java object an exception carries, and values larger than 64 KB.

## What it has to do

1. While one thread is inside a long Java call, a call from another thread of the same client runs at once.
2. An object made on one thread is usable from every thread of the client. Limits count per client.
3. All calls of one Python thread run on one JVM thread, for the life of the Python thread, and two Python threads have two JVM threads. Java code that keeps state in a `ThreadLocal` (Spark's active session, a transaction, a security context) depends on it.
4. When a Python thread ends, its JVM thread and its connection go with it.
5. A program with one thread pays nothing for any of this.

## Terms

- A **session** is what a client's threads share on the server: the objects, the limits, the callbacks, the callback connection.
- A **channel** is one connection of a session: a Unix socket, a shared memory file with the command, payload and response zones, and the JVM thread that serves them. A request and its response travel on one channel, and a channel carries one request at a time.

Today a session has exactly one channel, and the two words mean the same connection. After this change a session has one channel per Python thread that has called Java.

## The design

### 1. A channel per thread, joined to one session

The connection `connect()` makes is the session's **first channel**. It belongs to the client, not to a thread: it is used by the thread that connected, and it stays open until `close()`, because the session lives exactly as long as it does.

Any other thread gets a channel of its own with its first call. The client connects to a second listening socket of the server, `<socket>.join`, and says which session the channel is for:

```
client -> server: [4 bytes: protocol version][2 bytes: length][the session's shared memory path]
server -> client: the handshake a session gets (version, arena epoch, memory size, the path of
                  this channel's own shared memory file), or the connection is closed
```

The server starts a platform thread for the channel, as it does for a session, and that thread runs the same request loop. What differs is where its state comes from: the object IDs, the quota and the callbacks are the session's, the socket, the mapping and the Arrow state are its own.

The channel is kept in a `threading.local` of the client. That gives requirement 3 without bookkeeping: a thread always finds its own channel, and nobody else's.

Why a socket of its own, and not a message on the session's channel as the callback connection is opened (`OpenCallbackChannel`): that channel is the one that is busy. A thread that had to ask on it would wait for the long call it is trying not to wait for. Why not the server's main socket: the server speaks first there and has created a session, with a slot of `max_sessions` taken, before the client says anything; a client at its limit of sessions could not use threads. Naming the session by its shared memory path is what `Cancel` does already. The path is no secret and need not be: the join socket is owner-only and checks the peer's user like the main one, and the OS user is the trust boundary.

Why a socket and a mapping per thread at all, and not one connection that carries every thread's frames with an ID (the callback connection's way): a multiplexed connection needs a reader thread on each side that hands frames to whoever waits, which is two more thread wake-ups in each direction for every call. Moving sessions from virtual to platform threads was done to save one wake-up, about 30 μs a request. A thread's own socket costs none: the kernel wakes the thread that is waiting.

### 2. What is shared and what is not

| Per session (shared by all threads) | Per channel (one thread's own) |
|---|---|
| The objects (`sessionObjectIds`) and their limit | The socket and the JVM thread |
| The quota (`SessionQuota`): object count, Arrow bytes, idle time | The shared memory file: command, payload and response zones |
| The callbacks, and the callback connection | The Arrow state of that memory: the batch loaded in place, the arena epoch, the schema cache, the lent tables (`ArrowMemoryHandler`, `ArrowLoans`) |
| The client's queue of releases (`_pending_frees`), its reflection cache | What a cancel interrupts (`SessionControl`), the request log's ring buffer (`SessionObserver`) |

The class allowlist and the resource limits are the server's, as now.

On the server, `SessionContext` splits in two along that line. A thread-local names the channel the thread serves, and through it the session, where today one thread-local names both. The session's state is already written for several threads: threads that call Python on the callback connection register and release objects for a session while its own thread runs a request, so `sessionObjectIds` is a concurrent set, the quota is atomic, and a pack records exactly the IDs it registered (`packRecorder`).

In the client, the attributes that describe the connection (`sock`, `shm`, the payload bookkeeping, the lock) move into a channel object, and the request path looks up the calling thread's channel once.

Each channel keeps a lock, as the client has now, held for one exchange. Threads no longer wait for each other on it. It is there for the few cases where a thread touches another thread's channel: an `ArrowTableView` or an `ArrowLoan` handed to another thread, which copies out of, or ends a loan in, the memory of the channel it came from.

A callback made by the JVM thread that is running a request still travels in band, on that request's channel, to the Python thread that made the request. "In band" becomes a question about the calling JVM thread (is it serving a channel of the proxy's session?) and not about the one session thread. Every other JVM thread uses the callback connection, as now.

### 3. Arrow transfers from any thread

Every channel has the whole layout, payload zone included, so every Arrow transfer works on every thread as it does on the first, through the same code: a transfer writes the calling thread's payload zone and tells Java on the calling thread's channel. Two threads move two tables at the same time and neither waits.

The files are sparse. A channel that moves no tables uses the pages of its command and response zones and nothing more, so giving each thread the full size costs address space and not memory or disk. The server checks for free space before it creates one, as for a session's.

What follows from a payload zone per thread:

- A table read in place (`get_arrow_data()`, `arrow_from_java(copy=False)`) and a table lent to Java (`arrow_to_java(copy=False)`) live in the memory of the thread that made them. They may be used from any thread. A view ends on that channel's next transfer, as now, and both end when the channel closes: with `close()`, or when its thread ends.
- A `PayloadArena` from `get_payload_arena()` is the calling thread's. `send_arrow_buffers` refuses an arena of another channel: Java would read the calling thread's zone, where the data is not.
- The limit on Arrow bytes is the session's, so it counts every thread's transfers together.

The alternative was the memory a callback's transfer uses (`OpenCallMemory`): a small file per channel for commands and responses, and a second one made on demand for tables. It saves nothing a sparse file does not already save, and it needs a second version of every transfer. Only `arrow_to_java` and `arrow_from_java` have one today.

### 4. Cancel

`client.cancel()` interrupts every request the client has in flight, and returns `True` if there was at least one. `client.cancel(thread)` interrupts the request of that one thread.

With one thread this is what `cancel()` does today. With several, "the request that is running" no longer names one, and a caller who wants one names its thread. On the wire nothing new is needed: `Cancel` carries a shared memory path and a channel has one of its own, so the client sends one `Cancel` per channel on the short connection it already opens.

As now, a cancel is for a request a channel's own thread is running. Callbacks on the callback connection, and the commands they send, are not touched.

### 5. When a Python thread ends

Python drops a thread's `threading.local` values when the thread ends, and that is what closes the channel: its socket and its mapping. Nothing is sent. The server's thread for the channel reads the end of the stream, unmaps and deletes the channel's file, ends the tables lent from it, and exits. Its thread-locals end with it, which is why a channel's JVM thread is never reused for another Python thread.

The objects the thread made stay: they are the session's, and are released when their wrappers are collected, like any others.

The first channel is not dropped with its thread, since the session ends with it. If the thread that connected ends, the channel stays open and idle until `close()`.

A channel also ends when an exchange on it is interrupted after the command went out (a timeout, `KeyboardInterrupt`): the answer is still coming, and the next request would read it. Today that marks the client dead. With a channel per thread it ends that channel only, unless it is the first, and the thread's next call opens a new one, on a new JVM thread. The server drops what the old request registered for a response nobody can receive.

`close()` closes every channel. A thread that is inside a call on one of them gets `DeadConnectionError`.

### 6. Limits

- `gatun.limits.max_sessions` counts sessions. A channel that joins a session takes no slot, which is what the join socket is for.
- The idle timeout is the session's: it is idle when no channel is running a request and no callback is in flight, counted from the end of the last request on any channel. The reaper closes the first channel, which ends the session and all its channels. A channel whose thread is asleep is not closed on its own.
- The object limit and the Arrow limit are the session's.
- There is no limit on the channels of a session. Each is a platform thread and a sparse file; a join that cannot get a thread is refused, like a connection that cannot.

When the first channel closes, the session is over: the server releases its objects at once, as now, closes the other channels' sockets, and a request that is still running on one of them drops what it registers afterwards, the way a command from a callback does when it finds the session closed.

### 7. What is reused

From the callback connection:

- A connection that belongs to a session, takes no session slot and is refused to another user (`openCallbackListener`).
- Setting the session for a thread that is not the session's own, so that every lookup is checked against the right objects (`callOnChannel`).
- Exact tracking of what one thread registered (`packRecorder`) and dropping it when the response cannot be delivered or the session has ended (`runNested`, `discardNested`).
- Ordering releases against callback answers by count (`answers_sent`), which is already per session and does not care which channel a release arrives on.

Not reused: `NestedCommand` and `call_id`. They carry one thread's commands on a connection that many share, which is right for a callback that must be served by the thread waiting for it and too slow for a thread's ordinary calls (section 1).

A channel callback that outlives its answer sends its calls on "the client's own channel" today. That becomes the channel of the thread it runs on, so it no longer waits for a request another thread has in flight.

## The async client

Unchanged: one channel, and two calls awaited at the same time run one after the other, as now. A task is not a thread, and what requirement 3 promises (one JVM thread for all of a thread's calls) has no counterpart for tasks that share one event loop thread. A program that needs calls in flight at the same time uses the sync client from several threads.

## What this decides for the features that come first

**The Java object of an exception.** The `Throwable` is registered like any object a call returns: for the session, counted against its object limit, released when the Python exception is collected. Because objects are the session's and not the channel's, an exception raised on one thread can be examined on another, and no extra state per channel is needed.

**Values larger than 64 KB.** A command or a response that does not fit its 64 KB zone travels on the socket of the exchange it belongs to, after the length that announces it. That holds for every channel alike, and for the callback connection, whose frames already travel on a socket. Nothing is written to a payload zone, so a large value cannot land on a table that is lent or read in place, on any thread, and there is no second memory per thread to create, size and clean up. What bounds a value is the session's memory size and the resource limits; a value within 64 KB takes the path it takes today.

## Acceptance

| Requirement | Test |
|---|---|
| 1 | One thread sleeps 1.5 s in Java; `list.size()` on another thread of the same client returns in under 50 ms |
| 2 | An object made on one thread is used on another; the object limit is reached by two threads together |
| 3 | A Java `ThreadLocal` set by a thread is still set at its next call and unset for another thread; `Thread.currentThread().threadId()` is stable per Python thread and differs between two |
| 4 | 1,000 short-lived threads each make a call; afterwards the server has no thread and no file left from them |
| 5 | `list.size()`, a bound `Math.abs(-5)` and `ArrayList()` created and dropped, before and after, in alternation |
| Arrow | Two threads move tables at the same time; a view and a loan are used from another thread; an arena of another thread is refused |
| Callbacks | A callback registered on one thread is called in band during another thread's request, and from a thread Java started |
| Cancel | `cancel()` interrupts two requests in flight; `cancel(thread)` interrupts one and leaves the other |
