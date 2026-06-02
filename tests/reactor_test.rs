use std::mem;
use std::time::Duration;

use thrust_rs::reactor::kqueue::Kqueue;
use thrust_rs::reactor::registration::RegistrationState;
use thrust_rs::reactor::{Interest, Reactor, Token};

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

    let token_1 = reactor
        .register(r_fd, Interest::READ, waker_1, RegistrationState::new())
        .unwrap();

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

    let token_2 = reactor
        .register(r_fd, Interest::READ, waker_2, RegistrationState::new())
        .unwrap();

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
