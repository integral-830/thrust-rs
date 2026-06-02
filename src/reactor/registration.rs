use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::reactor::Token;

const INVALID_TOKEN: usize = usize::MAX;

pub struct RegistrationState {
    token: AtomicUsize,
}

impl RegistrationState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            token: AtomicUsize::new(INVALID_TOKEN),
        })
    }

    pub fn get_token(&self) -> Option<Token> {
        let value = self.token.load(std::sync::atomic::Ordering::Acquire);
        if value == INVALID_TOKEN {
            None
        } else {
            Some(Token(value))
        }
    }

    pub fn set_token(&self, token: Token) {
        self.token
            .store(token.0, std::sync::atomic::Ordering::Release);
    }

    pub fn clear_token(&self) {
        self.token
            .store(INVALID_TOKEN, std::sync::atomic::Ordering::Release);
    }

    pub fn is_registered(&self) -> bool {
        self.token.load(std::sync::atomic::Ordering::Acquire) != INVALID_TOKEN
    }
}
