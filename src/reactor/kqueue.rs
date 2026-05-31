use std::io;
use std::os::fd::RawFd;
use std::ptr::null_mut;
use std::time::Duration;
use std::{ffi::c_void, ptr::null};

use libc::{kevent, EINTR, EVFILT_READ, EVFILT_WRITE, EV_ADD, EV_CLEAR, EV_DELETE};

pub struct Kqueue {
    pub fd: RawFd,
}

impl Kqueue {
    pub fn new() -> io::Result<Self> {
        let fd = unsafe { libc::kqueue() };

        if fd == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self { fd })
        }
    }

    pub fn get_raw_fd(&self) -> RawFd {
        self.fd
    }

    pub fn add_read(&self, fd: RawFd, token: usize) -> io::Result<()> {
        let event = make_event(fd as usize, EVFILT_READ, EV_ADD | EV_CLEAR, token);
        let add_read_resp = unsafe { libc::kevent(self.fd, &event, 1, null_mut(), 0, null_mut()) };
        if add_read_resp == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    pub fn add_write(&self, fd: RawFd, token: usize) -> io::Result<()> {
        let event = make_event(fd as usize, EVFILT_WRITE, EV_ADD | EV_CLEAR, token);
        let add_write_resp = unsafe { libc::kevent(self.fd, &event, 1, null_mut(), 0, null_mut()) };
        if add_write_resp == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    pub fn delete(&self, fd: RawFd) -> io::Result<()> {
        let changes = [
            make_event(fd as usize, EVFILT_READ, EV_DELETE, 0),
            make_event(fd as usize, EVFILT_WRITE, EV_DELETE, 0),
        ];

        let del_resp = unsafe {
            kevent(
                self.fd,
                changes.as_ptr(),
                changes.len() as _,
                null_mut(),
                0,
                null_mut(),
            )
        };

        if del_resp == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn wait(
        &self,
        events: &mut [libc::kevent],
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        let time_spec = timeout.map(|t| libc::timespec {
            tv_sec: t.as_secs() as _,
            tv_nsec: t.subsec_nanos() as _,
        });

        let timeout_ptr = match &time_spec {
            Some(t_spec) => t_spec as *const libc::timespec,
            None => null(),
        };
        let wait_resp = unsafe {
            libc::kevent(
                self.fd,
                null(),
                0,
                events.as_mut_ptr(),
                events.len() as _,
                timeout_ptr,
            )
        };

        if wait_resp == -1 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(EINTR) {
                return Ok(0);
            }
        }
        Ok(wait_resp as usize)
    }
}

pub fn make_event(ident: usize, filter: i16, flags: u16, token: usize) -> libc::kevent {
    libc::kevent {
        ident: ident as libc::uintptr_t,
        filter,
        flags,
        fflags: 0,
        data: 0,
        udata: token as *mut libc::c_void,
    }
}

impl Drop for Kqueue {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}
