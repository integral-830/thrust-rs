# thrust-rs

A small async runtime built from scratch in Rust.

This project implements a single-threaded cooperative executor, a hand-rolled
`RawWaker`, and a macOS/BSD `kqueue` reactor for non-blocking TCP I/O. It does
not use Tokio, async-std, mio, or polling abstractions from external runtime
crates.

The runtime is intentionally small so the mechanics are visible:

```text
spawn(future)
  -> Box::pin(future)
  -> Arc<Task>
  -> mpsc ready queue
  -> Executor polls task
  -> custom RawWaker re-enqueues task on wake

socket WouldBlock
  -> register fd with kqueue
  -> store task waker in Reactor registry
  -> kqueue readiness event
  -> wake task
  -> retry syscall
```

## Current Features

- Single-threaded async executor.
- Custom `RawWaker` implementation.
- Runtime facade with `Runtime::spawn`, `Runtime::run`, and `Runtime::block_on`.
- Thread-local runtime context for nested `spawn(...)` and reactor access.
- `kqueue` reactor for readiness-based I/O.
- One-shot registration state per readiness interest.
- Async TCP listener and stream wrappers.
- Futures for `accept`, `connect`, `read`, `write`, `yield_once`, `countdown`,
  and wake-latency measurement.
- Integration tests for connect, accept/read/write, fd cleanup, registration
  lifecycle, and executor stress cases.

## Platform

The reactor uses `kqueue`, so the I/O implementation targets macOS and BSD-like
systems.

Linux support would need an `epoll` backend. Windows support would need an IOCP
or readiness backend.

## Project Layout

```text
src/
  executor/
    mod.rs          Executor and ready queue
    raw_waker.rs    Arc<Task>-backed RawWaker vtable
    tasks.rs        Task container
  reactor/
    mod.rs          Reactor registry and readiness dispatch
    kqueue.rs       Raw kqueue wrapper
    registration.rs Shared one-shot registration state
  runtime/
    mod.rs          Runtime facade and block_on loop
    spawn.rs        Thread-local nested spawn support
    with_reactor.rs Thread-local reactor access
  net/
    tcp_listener.rs AsyncTcpListener
    tcp_stream.rs   AsyncTcpStream
  futures/
    accept.rs
    connect.rs
    read.rs
    write.rs
    yield_once.rs
    countdown.rs
    immediate.rs
    wake_latency.rs

examples/
  echo_server.rs
  echo_client.rs
  http_server.rs

tests/
  connect_test.rs
  integration_test.rs
  reactor_test.rs
  registration_test.rs
  runtime_test.rs
  stress_test.rs
```

## Runtime Architecture

### Task

Each spawned future is stored in a `Task`:

```rust
pub struct Task {
    pub future: Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>,
    pub sender: Sender<Arc<Task>>,
}
```

- `Pin<Box<dyn Future>>` keeps the future heap-allocated and immovable.
- `Mutex` gives mutable access while polling.
- `sender` lets the custom waker push the task back into the ready queue.

### Executor

The executor owns:

- an `mpsc` sender/receiver ready queue,
- an `Arc<Reactor>`,
- an atomic task counter.

The main loop is:

```text
drain ready queue
  -> poll each task
  -> Ready: decrement task count
  -> Pending: wait for wakeup

if tasks still exist and no ready work was done
  -> reactor.run_once(...)
```

This keeps CPU-bound self-waking futures moving while allowing socket futures to
park on `kqueue` readiness.

### RawWaker

The runtime builds a `Waker` from a custom `RawWakerVTable`.

| Method        | Behavior                                                           |
| ------------- | ------------------------------------------------------------------ |
| `clone`       | Clones the underlying `Arc<Task>`.                                 |
| `wake`        | Reconstructs the `Arc<Task>` and sends a cloned task to the queue. |
| `wake_by_ref` | Sends a cloned task without consuming the original waker.          |
| `drop`        | Drops the `Arc<Task>` created by `Arc::into_raw`.                  |

The important invariant is that every `Arc::from_raw` is balanced. Getting this
wrong means leaking tasks or causing use-after-free bugs.

## Reactor Architecture

The reactor wraps a `Kqueue` and a registry:

```rust
pub struct Reactor {
    pub kq: Kqueue,
    pub registry: Mutex<HashMap<Token, Registration>>,
    pub advance_token: AtomicUsize,
    pub wake_ident: usize,
}
```

Each registration stores:

```rust
pub struct Registration {
    pub fd: RawFd,
    pub interest: Interest,
    pub waker: Waker,
    pub state: Arc<RegistrationState>,
}
```

### Registration Flow

When a non-blocking socket returns `WouldBlock`:

1. The future asks the reactor to register the fd for `READ` or `WRITE`.
2. The reactor allocates a `Token`.
3. The token is stored in `RegistrationState`.
4. A `kevent` is added with `EV_ADD | EV_CLEAR`.
5. The task returns `Poll::Pending`.
6. `Reactor::run_once(...)` waits for kqueue events.
7. On readiness, the reactor removes the registry entry, clears the state, and
   wakes the task.
8. The future is polled again and retries the original socket operation.

Registrations are one-shot at the runtime layer. After a readiness event, the
future must attempt the syscall again and re-register if it still gets
`WouldBlock`.

### RegistrationState

`RegistrationState` is shared between the socket/future and reactor:

```rust
pub struct RegistrationState {
    token: AtomicUsize,
}
```

It lets a future know whether it already has an active reactor registration.
This prevents duplicate registrations for the same pending read/write/accept
operation.

## TCP I/O

### AsyncTcpListener

`AsyncTcpListener::bind(...)` creates a non-blocking `TcpListener`.

`accept().await`:

- calls `TcpListener::accept()`,
- returns immediately on success,
- registers `READ` interest on `WouldBlock`,
- wakes when a client connection is ready.

### AsyncTcpStream

`AsyncTcpStream` wraps a non-blocking `TcpStream`.

Available operations:

- `AsyncTcpStream::connect(addr)?.await`
- `stream.read(&mut buf).await`
- `stream.write(bytes).await`

The stream tracks separate registration state for reads and writes, so read and
write readiness do not overwrite each other.

## Usage Examples

### Run a Simple Future

```rust
use thrust_rs::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();

    let value = rt.block_on(async {
        42
    });

    assert_eq!(value, 42);
}
```

### Spawn Tasks Before Running

```rust
use thrust_rs::futures::yield_once::YieldOnce;
use thrust_rs::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();

    rt.spawn(async {
        YieldOnce::new().await;
        println!("task finished");
    });

    rt.run();
}
```

### Spawn From Inside the Runtime

Use `thrust_rs::runtime::spawn::spawn(...)` when you are already inside
`Runtime::run()` or `Runtime::block_on(...)`.

```rust
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use thrust_rs::runtime::spawn::spawn;
use thrust_rs::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let count_for_task = count.clone();

    rt.block_on(async move {
        spawn(async move {
            count_for_task.store(1, Ordering::SeqCst);
        });
    });

    assert_eq!(count.load(Ordering::SeqCst), 1);
}
```

`Runtime::block_on(...)` drains spawned tasks before returning.

### Use Runtime::spawn With run()

Use `Runtime::spawn(...)` to schedule top-level tasks before starting the
executor loop.

```rust
use thrust_rs::runtime::Runtime;

async fn worker(id: usize) {
    println!("worker {id} started");
}

fn main() {
    let rt = Runtime::new().unwrap();

    rt.spawn(worker(1));
    rt.spawn(worker(2));

    rt.run();
}
```

`Runtime::run()` exits when the runtime task counter reaches zero.

### Self-Waking Futures

`YieldOnce` and `Countdown` are small futures used by tests and benchmarks to
exercise the executor without I/O.

```rust
use thrust_rs::futures::countdown::Countdown;
use thrust_rs::futures::yield_once::YieldOnce;
use thrust_rs::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        YieldOnce::new().await;
        Countdown::new(5).await;
    });
}
```

### Manual Executor Construction

Most code should use `Runtime`, but tests and benchmarks can construct the
executor directly.

```rust
use std::sync::Arc;

use thrust_rs::executor::Executor;
use thrust_rs::reactor::Reactor;

fn main() {
    let reactor = Arc::new(Reactor::new().unwrap());
    let executor = Executor::new(reactor);

    executor.spawn(async {
        println!("polled by the custom executor");
    });

    executor.run();
}
```

### Echo Server

```rust
use thrust_rs::net::tcp_listener::AsyncTcpListener;
use thrust_rs::net::tcp_stream::AsyncTcpStream;
use thrust_rs::runtime::spawn::spawn;
use thrust_rs::runtime::Runtime;

async fn handle_connection(mut stream: AsyncTcpStream) {
    let mut buf = [0u8; 4096];

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                if stream.write(&buf[..n]).await.is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

async fn server() {
    let mut listener = AsyncTcpListener::bind("127.0.0.1:8080").unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => spawn(handle_connection(stream)),
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(server());
}
```

### Echo Client

```rust
use thrust_rs::net::tcp_stream::AsyncTcpStream;
use thrust_rs::runtime::Runtime;

async fn client() {
    let addr = "127.0.0.1:8080".parse().unwrap();
    let mut stream = AsyncTcpStream::connect(addr).unwrap().await.unwrap();

    stream.write(b"hello from thrust-rs").await.unwrap();

    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();

    println!("echoed: {}", String::from_utf8_lossy(&buf[..n]));
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(client());
}
```

## Running Examples

Run the echo server:

```sh
cargo run --example echo_server
```

In another terminal:

```sh
cargo run --example echo_client
```

Run the HTTP server:

```sh
cargo run --example http_server
```

Then open:

```text
http://127.0.0.1:8080
```

## Tests

Run the test suite:

```sh
cargo test
```

Some tests bind local TCP sockets and exercise `kqueue`. In restricted
sandboxes, those tests may fail with `Operation not permitted`; run them in a
normal local shell.

The test suite covers:

- executor self-waking futures,
- runtime `block_on` and nested spawn,
- kqueue pipe readiness,
- one-shot reactor registration,
- `RegistrationState` lifecycle,
- async TCP connect success/refusal/cancellation,
- fd cleanup after dropped connect futures,
- end-to-end local TCP echo.

## Benchmarks

Criterion benchmarks live in `benches/exec_benchmark.rs`.

Compile the benchmark target:

```sh
cargo bench --no-run
```

Run benchmarks:

```sh
cargo bench
```

Current benchmark cases include:

- spawn setup for 10k and 100k tasks,
- `YieldOnce` for 10k and 100k tasks,
- `Countdown` with 5 and 50 pending polls,
- wake latency measurement.

The benchmark numbers in the old Week 1 README were from the executor-only
baseline. Since the runtime now includes a reactor and the executor constructor
takes an `Arc<Reactor>`, rerun `cargo bench` before quoting current numbers.

## Complete Benchmark History

These are observed local benchmark and stability results from the current
kqueue-based runtime and TCP examples.

### CPU Idle Verification

Server running with no clients connected.

Measured with:

```sh
ps -o %cpu -p <pid>
```

Observed repeatedly:

```text
%CPU
0.0
```

Result:

- No busy waiting.
- No spin loops.
- Reactor sleeps correctly while idle.

### FD Stability Verification

Measured with:

```sh
lsof -p <pid> | wc -l
```

Observed:

```text
Before load:
9 FDs

After load:
9 FDs
```

Also verified using `count_fds()` based on `/dev/fd`.

Result:

```text
FD count always returned to baseline.
```

### Connection Churn Benchmark

Command:

```sh
time (
for i in $(seq 1 50000); do
(
    echo "hello" | nc -w 1 localhost 8080 >/dev/null
) &
done
wait
)
```

Result:

```text
104.40s user
186.82s system
497% cpu
58.538 total
```

Equivalent:

```text
50,000 successful connections
58.538 seconds

approximately 854 connections/sec
```

Result:

- No crashes.
- No leaks.
- No hangs.
- No deadlocks.

### Concurrent Client Validation

Command pattern:

```sh
for i in $(seq 1 10000); do
(
    echo "hello$i" | nc -w 2 localhost 8080 >/dev/null
) &
done
wait
```

Result:

```text
10,000 concurrent clients completed successfully.
```

### wrk Benchmark 1

Command:

```sh
wrk -t4 -c100 -d10s http://localhost:8080
```

Result:

```text
146,524 requests
10.01s

Requests/sec:
14,633.66

Transfer/sec:
1.35 MB/s
```

### wrk Benchmark 2

Command:

```sh
wrk -t4 -c100 -d10s --latency http://localhost:8080
```

Result:

```text
Requests/sec:
13,881.36

Latency:
p50: 3.96 ms
p75: 5.35 ms
p90: 6.25 ms
p99: 39.14 ms

Total requests:
139,343
```

### wrk Benchmark 3

Command:

```sh
wrk -t4 -c100 -d10s --latency http://localhost:8080
```

Result:

```text
Requests/sec:
14,632.03

Latency:
p50: 3.84 ms
p75: 5.17 ms
p90: 6.01 ms
p99: 31.96 ms

Total requests:
147,245
```

### wrk Benchmark 4

Command:

```sh
wrk -t4 -c100 -d10s --latency http://localhost:8080
```

Result:

```text
Requests/sec:
16,030.53

Latency:
p50: 3.53 ms
p75: 4.73 ms
p90: 5.49 ms
p99: 29.00 ms

Total requests:
160,606
```

### wrk Benchmark 5

Command:

```sh
wrk -t4 -c100 -d10s --latency http://localhost:8080
```

Result:

```text
Requests/sec:
16,006.28

Latency:
p50: 3.54 ms
p75: 4.73 ms
p90: 5.49 ms
p99: 26.76 ms

Total requests:
160,613
```

### wrk Benchmark 6

Command:

```sh
wrk -t8 -c1000 -d30s --latency http://localhost:8080
```

Result:

```text
Requests/sec:
14,365.43

Latency:
p50: 10.09 ms
p75: 12.54 ms
p90: 15.27 ms
p99: 34.63 ms

Total requests:
432,427

Socket errors:
read=161
```

### wrk Benchmark 7

Command:

```sh
wrk -t8 -c5000 -d30s --latency http://localhost:8080
```

Result:

```text
Requests/sec:
14,203.69

Latency:
p50: 11.52 ms
p75: 15.12 ms
p90: 20.77 ms
p99: 61.25 ms

Total requests:
427,465

Socket errors:
connect=2453
read=1567
```

### wrk Benchmark 8

Command:

```sh
wrk -t8 -c1000 -d5m --latency http://localhost:8080
```

Result:

```text
Requests/sec:
14,047.99

Latency:
p50: 9.99 ms
p75: 12.30 ms
p90: 14.66 ms
p99: 32.32 ms

Total requests:
4,215,594

Socket errors:
read=1947
```

### wrk Benchmark 9

Command:

```sh
wrk -t8 -c5000 -d5m --latency http://localhost:8080
```

Result:

```text
Requests/sec:
14,243.44

Latency:
p50: 10.73 ms
p75: 13.15 ms
p90: 16.05 ms
p99: 44.59 ms

Total requests:
4,273,909

Socket errors:
connect=2453
read=10992
```

### Samply Profile Before Keep-Alive

Top self-time:

```text
accept()      59.3%
kevent()      29.9%
send()         3.8%
close()        3.2%
recv()         1.4%
```

Conclusion:

```text
Connection establishment dominated CPU usage.
```

### Keep-Alive Benchmark

After implementing:

```http
Connection: keep-alive
```

and persistent request handling.

Command:

```sh
wrk -t8 -c1000 -d30s --latency http://localhost:8080
```

Result:

```text
Requests/sec:
173,300.34

Total Requests:
5,211,577

Transfer/sec:
12.56 MB/s

Latency:
p50: 5.72 ms
p75: 5.81 ms
p90: 5.91 ms
p99: 6.43 ms
```

Improvement:

```text
approximately 12.4x throughput increase
approximately 5x p99 improvement
```

### Samply Profile With Keep-Alive

Top self-time:

```text
recv()      43.08%
send()      34.64%
kevent()    19.33%
```

Runtime components:

```text
Reactor::run_once          0.63%
Reactor::register          0.34%
Channel::send              0.25%
WriteFuture::poll          0.23%
AsyncTcpStream::poll_read  0.19%
Mutex::lock                0.08%
Channel::try_recv          0.07%
raw_waker::wake            0.07%
Executor::run_until_empty  0.03%
```

Measured runtime overhead:

```text
< 2%
```

### Resource Stability During Benchmarks

Verified repeatedly:

- FD count returns to baseline.
- Memory returns to baseline.
- Idle CPU returns to 0%.
- No observable resource leaks.
- No executor deadlocks.
- No reactor deadlocks.
- No task leaks.
- No registration leaks.
- No crashes during stress testing.

## Current Limitations

- Single-threaded runtime; no work stealing.
- No timers or sleep future yet.
- `kqueue` only; no `epoll` or IOCP backend.
- `std::sync::mpsc` ready queue.
- `Mutex` around every task future, even though the executor is single-threaded.
- No task cancellation API beyond dropping futures/resources.
- No task priorities.
- TCP connect currently implements IPv4 only.
- Public API is experimental and intentionally low-level.
