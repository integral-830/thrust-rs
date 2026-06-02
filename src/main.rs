#![allow(unused)]

use std::future::{self, Future};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};
use std::{io, mem};

use thrust_rs::futures::connect::ConnectFuture;
use thrust_rs::futures::countdown::Countdown;
use thrust_rs::futures::yield_once::YieldOnce;
use thrust_rs::net::tcp_listener::AsyncTcpListener;
use thrust_rs::net::tcp_stream::AsyncTcpStream;
use thrust_rs::reactor::kqueue::Kqueue;
use thrust_rs::reactor::registration::RegistrationState;
use thrust_rs::reactor::{Interest, Reactor, Token};
use thrust_rs::runtime::executor::Executor;
use thrust_rs::runtime::raw_waker::waker_for_task;
use thrust_rs::runtime::tasks::Task;

fn main() {
    let executor = Executor::new();
    executor.spawn(YieldOnce::default());
    executor.run();
}
