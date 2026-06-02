use std::cell::RefCell;
use std::sync::Arc;

use crate::reactor::Reactor;

thread_local! {
    static REACTOR: RefCell<Option<Arc<Reactor>>> = const{RefCell::new(None)};
}

pub fn set_reactor(reactor: Arc<Reactor>) {
    REACTOR.with(|r| {
        *r.borrow_mut() = Some(reactor);
    });
}

pub fn with_reactor<T>(f: impl FnOnce(&Arc<Reactor>) -> T) -> T {
    REACTOR.with(|r| {
        let reactor = r.borrow();
        let reactor = reactor.as_ref().expect(
            "No runtime found\
            Try calling from Runtime::run()",
        );
        f(reactor)
    })
}

pub fn clear_reactor() {
    REACTOR.with(|r| *r.borrow_mut() = None);
}
