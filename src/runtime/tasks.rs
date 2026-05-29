use std::{
    future::Future,
    pin::Pin,
    sync::{mpsc::Sender, Arc, Mutex},
};

pub struct Task {
    pub future: Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>,
    pub sender: Sender<Arc<Task>>,
}
