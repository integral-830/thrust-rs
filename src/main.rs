#![allow(unused)]

use std::future::{self, Future};
use std::mem;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use thrust_rs::futs::countdown_fut::Countdown;
use thrust_rs::futs::yield_once::YieldOnce;
use thrust_rs::reactor::kqueue::Kqueue;
use thrust_rs::reactor::{Interest, Reactor};
use thrust_rs::runtime::executor::Executor;
use thrust_rs::runtime::raw_waker::waker_for_task;
use thrust_rs::runtime::tasks::Task;

fn main() {
    let executor = Executor::new();
    executor.spawn(YieldOnce::default());
    executor.run();
}

#[test]
fn yield_once_test() {
    let (tx, rx) = mpsc::channel::<Arc<Task>>();

    let mut yield_one_fut = Box::pin(YieldOnce::default());

    let task = Arc::new(Task {
        future: Mutex::new(yield_one_fut),
        sender: tx,
    });

    assert_eq!(Arc::strong_count(&task), 1);

    let waker = waker_for_task(task.clone());
    assert_eq!(Arc::strong_count(&task), 2);
    let mut cx = Context::from_waker(&waker);

    {
        let mut future = task.future.lock().unwrap();
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
    }
    assert!(rx.try_recv().is_ok());
    {
        let mut future = task.future.lock().unwrap();
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Ready(()));
    }
    drop(waker);
    assert_eq!(Arc::strong_count(&task), 1);
}

#[test]
fn yield_once_thousand() {
    let executor = Executor::new();
    let count = Arc::new(AtomicU32::new(0));

    for _ in 0..1000 {
        let count = count.clone();
        executor.spawn(async move {
            YieldOnce::default().await;
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    executor.run();
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1000);
}

#[test]
fn stress_10k_each_5_yields() {
    let executor = Executor::new();
    let count = Arc::new(AtomicU32::new(0));
    let start = Instant::now();

    for _ in 0..10000 {
        let count = count.clone();
        executor.spawn(async move {
            Countdown::new(5).await;
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    executor.run();
    let elapsed = start.elapsed();
    println!("completed 10,000 tasks in {elapsed:?}");
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 10000);
}

#[test]
fn yield_once_100k() {
    let executor = Executor::new();
    let count = Arc::new(AtomicU32::new(0));
    let start = Instant::now();

    for _ in 0..100_000 {
        let count = count.clone();
        executor.spawn(async move {
            YieldOnce::default().await;
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    executor.run();
    let elapsed = start.elapsed();
    println!("completed 10,000 tasks in {elapsed:?}");
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 100_000);
}
#[test]
fn yield_once_1m() {
    let executor = Executor::new();
    let count = Arc::new(AtomicU32::new(0));
    let start = Instant::now();

    for _ in 0..1_000_000 {
        let count = count.clone();
        executor.spawn(async move {
            YieldOnce::default().await;
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    executor.run();
    let elapsed = start.elapsed();
    println!("completed 10,000 tasks in {elapsed:?}");
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1_000_000);
}

#[test]
fn pipe_kq_event() {
    let kq = Kqueue::new().unwrap();
    let mut fds = [0; 2];
    let fd_res = unsafe { libc::pipe(fds.as_mut_ptr()) };
    assert_eq!(fd_res, 0);
    let r_fd = fds[0];
    let w_fd = fds[1];
    kq.add_read(r_fd, 1).unwrap();
    let t_byte = [21u8];
    let write_resp = unsafe { libc::write(w_fd, t_byte.as_ptr() as *const libc::c_void, 1) };
    assert_eq!(write_resp, 1);
    let mut events = [unsafe { mem::zeroed::<libc::kevent>() }; 10];

    let res = kq.wait(&mut events, Some(Duration::from_secs(1))).unwrap();
    assert_eq!(res, 1);

    let r_ev = &events[0];
    let ident = r_ev.ident;
    assert_eq!(ident, r_fd as libc::uintptr_t);
    assert_eq!(r_ev.filter, libc::EVFILT_READ);

    unsafe {
        libc::close(r_fd);
        libc::close(w_fd);
    }
}

#[test]
fn reactor_one_shot_registration() {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::{Wake, Waker},
    };

    struct CountWaker {
        count: Arc<AtomicUsize>,
    }

    impl Wake for CountWaker {
        fn wake(self: Arc<Self>) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    let reactor = Reactor::new().unwrap();

    let mut fds = [0; 2];
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);

    let r_fd = fds[0];
    let w_fd = fds[1];

    //
    // Part 1
    //

    let wake_count_1 = Arc::new(AtomicUsize::new(0));

    let waker_1 = Waker::from(Arc::new(CountWaker {
        count: wake_count_1.clone(),
    }));

    let token_1 = reactor.register(r_fd, Interest::READ, waker_1).unwrap();

    let byte = [1u8];

    unsafe {
        libc::write(w_fd, byte.as_ptr() as *const libc::c_void, 1);
    }

    reactor.run_once(Some(100));

    assert_eq!(wake_count_1.load(Ordering::SeqCst), 1);

    assert!(!reactor.registry.lock().unwrap().contains_key(&token_1));

    //
    // Drain pipe
    //

    let mut buf = [0u8; 1];

    unsafe {
        libc::read(r_fd, buf.as_mut_ptr() as *mut libc::c_void, 1);
    }

    //
    // Part 2
    //

    let wake_count_2 = Arc::new(AtomicUsize::new(0));

    let waker_2 = Waker::from(Arc::new(CountWaker {
        count: wake_count_2.clone(),
    }));

    let token_2 = reactor.register(r_fd, Interest::READ, waker_2).unwrap();

    assert_ne!(token_1, token_2);

    unsafe {
        libc::write(w_fd, byte.as_ptr() as *const libc::c_void, 1);
    }

    reactor.run_once(Some(100));

    assert_eq!(wake_count_2.load(Ordering::SeqCst), 1);

    assert!(!reactor.registry.lock().unwrap().contains_key(&token_2));

    //
    // Part 3
    //

    assert_eq!(wake_count_1.load(Ordering::SeqCst), 1);

    assert_eq!(wake_count_2.load(Ordering::SeqCst), 1);

    unsafe {
        libc::close(r_fd);
        libc::close(w_fd);
    }
}
