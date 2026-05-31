#![allow(unused)]

use std::future::{self, Future};
use std::mem;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use fut::futs::countdown_fut::Countdown;
use fut::futs::immediate_fut::ImmediateReady;
use fut::futs::yield_once::YieldOnce;
use fut::reactor::kqueue::{self, Kqueue};
use fut::runtime::executor::Executor;
use fut::runtime::raw_waker::waker_for_task;
use fut::runtime::tasks::Task;

fn main() {
    // let (tx, rx) = mpsc::channel::<Arc<Task>>();
    // let mut _fut = Box::pin(ImmediateReady::new(42));

    // let mut yield_one_fut = Box::pin(YieldOnce::default());
    // let mut _countdown_fut = Box::pin(Countdown::new(4));

    // let task = Arc::new(Task {
    //     future: Mutex::new(yield_one_fut),
    //     sender: tx,
    // });

    // let waker = waker_for_task(task.clone());
    // let mut cx = Context::from_waker(&waker);

    // loop {
    //     let poll_result = {
    //         let mut future = task.future.lock().unwrap();
    //         future.as_mut().poll(&mut cx)
    //     };
    //     match poll_result {
    //         Poll::Ready(v) => {
    //             println!("Fut completed.");
    //             break;
    //         }
    //         Poll::Pending => {
    //             println!("Fut is pending...");
    //         }
    //     }
    // }
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
