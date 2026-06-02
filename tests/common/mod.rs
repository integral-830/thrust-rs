use std::sync::Arc;

use thrust_rs::reactor::Reactor;
use thrust_rs::runtime::with_reactor::set_reactor;

pub fn install_test_runtime() -> Arc<Reactor> {
    let reactor = Arc::new(Reactor::new().unwrap());

    set_reactor(reactor.clone());

    reactor
}
