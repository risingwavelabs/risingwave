use pgwire::pg_protocol::CURRENT_SESSION;

use super::SessionImpl;

fn with_current_session<R>(f: impl FnOnce(&SessionImpl) -> R) -> Option<R> {
    CURRENT_SESSION
        .try_with(|s| s.upgrade().and_then(|s| s.downcast_ref().map(f)))
        .ok()
        .flatten()
}

/// Send a notice to the user, if currently in the context of a session.
pub(crate) fn notice_to_user(str: impl Into<String>) {
    let _ = with_current_session(|s| s.notice_to_user(str));
}
