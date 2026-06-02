use thrust_rs::reactor::registration::RegistrationState;
use thrust_rs::reactor::Token;

#[test]
fn registration_state_lifecycle() {
    let state = RegistrationState::new();

    assert_eq!(state.get_token(), None);
    assert!(!state.is_registered());

    let token = Token(42);

    state.set_token(token);

    assert_eq!(state.get_token(), Some(Token(42)));
    assert!(state.is_registered());

    state.clear_token();

    assert_eq!(state.get_token(), None);
    assert!(!state.is_registered());
}

#[test]
fn registration_state_shared_between_arcs() {
    let state = RegistrationState::new();

    let clone = state.clone();

    state.set_token(Token(7));

    assert_eq!(clone.get_token(), Some(Token(7)));
    assert!(clone.is_registered());

    clone.clear_token();

    assert_eq!(state.get_token(), None);
    assert!(!state.is_registered());
}

#[test]
fn registration_state_cleared_when_reactor_consumes_event() {
    let state = RegistrationState::new();

    state.set_token(Token(99));

    assert!(state.is_registered());

    state.clear_token();

    assert!(!state.is_registered());
    assert_eq!(state.get_token(), None);
}
