use std::io;
use std::os::fd::RawFd;
use std::ptr::{null, null_mut};
use std::time::Duration;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Mutex},
    task::Waker,
};

use libc::{
    kevent, uintptr_t, EVFILT_READ, EVFILT_USER, EVFILT_WRITE, EV_ADD, EV_CLEAR, NOTE_TRIGGER,
};

use crate::reactor::kqueue::Kqueue;

use self::kqueue::make_event;

pub mod kqueue;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Token(pub usize);

pub enum Interest {
    READ,
    WRITE,
}

pub struct Registration {
    pub fd: RawFd,
    pub interest: Interest,
    pub waker: Waker,
}

pub struct Reactor {
    pub kq: Kqueue,
    pub registry: Mutex<HashMap<Token, Registration>>,
    pub advance_token: AtomicUsize,
    pub wake_ident: usize,
}

impl Reactor {
    pub fn new() -> io::Result<Self> {
        let kq = Kqueue::new().unwrap();
        let wake_ident = usize::MAX;
        let event = make_event(wake_ident, EVFILT_USER, EV_ADD | EV_CLEAR, wake_ident);
        let ev_resp = unsafe { libc::kevent(kq.fd, &event, 1, null_mut(), 0, null()) };
        if ev_resp == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self {
            kq,
            registry: Mutex::new(HashMap::new()),
            advance_token: AtomicUsize::new(0),
            wake_ident,
        })
    }

    pub fn register(&self, fd: RawFd, interest: Interest, waker: Waker) -> io::Result<Token> {
        let token = Token(self.fetch_and_advance_token());
        let filters = match interest {
            Interest::READ => EVFILT_READ,
            Interest::WRITE => EVFILT_WRITE,
        };
        let event = make_event(fd as usize, filters, EV_ADD | EV_CLEAR, token.0);

        let resp = unsafe { libc::kevent(self.kq.fd, &event, 1, null_mut(), 0, null()) };

        if resp == -1 {
            return Err(io::Error::last_os_error());
        }
        self.registry.lock().unwrap().insert(
            token,
            Registration {
                fd,
                interest,
                waker,
            },
        );
        self.trigger_wake()?;
        Ok(token)
    }

    pub fn fetch_and_advance_token(&self) -> usize {
        self.advance_token
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn trigger_wake(&self) -> io::Result<()> {
        let event = libc::kevent {
            ident: self.wake_ident as uintptr_t,
            filter: EVFILT_USER,
            flags: 0,
            fflags: NOTE_TRIGGER,
            data: 0,
            udata: null_mut(),
        };

        let resp = unsafe { kevent(self.kq.fd, &event, 1, null_mut(), 0, null()) };
        if resp == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn deregister_fd(&self, fd: RawFd, token: Token) {
        self.registry.lock().unwrap().remove(&token);
        self.kq.delete(fd).ok();
    }

    pub fn run_once(&self, timeout_ms: Option<u64>) {
        let mut events = [unsafe { std::mem::zeroed::<libc::kevent>() }; 64];
        let n = self
            .kq
            .wait(
                &mut events,
                Some(Duration::from_millis(timeout_ms.unwrap())),
            )
            .unwrap();
        let mut registry = self.registry.lock().unwrap();
        for event in &events[..n] {
            let token = Token(event.data as usize);
            if token.0 == self.wake_ident {
                continue;
            }
            if let Some(registration) = registry.remove(&token) {
                registration.waker.wake();
            }
        }
    }
}
