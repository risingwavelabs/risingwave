use std::any::Any;
use std::sync::Weak;

type ProcessId = i32;
type SecretKey = i32;

pub type SessionId = (ProcessId, SecretKey);

pub trait MinSession: Any + Send + Sync {
    fn id(&self) -> SessionId;
}

pub fn with_current<R>(f: impl FnOnce(&dyn MinSession) -> R) -> Option<R> {
    CURRENT_SESSION
        .try_with(|s| s.upgrade().map(|s| f(s.as_ref())))
        .ok()
        .flatten()
}

pub fn with_current_as<T: 'static, R>(f: impl FnOnce(&T) -> R) -> Option<R> {
    with_current(|s| (s as &dyn Any).downcast_ref::<T>().map(f)).flatten()
}

tokio::task_local! {
    /// The current session. Concrete type is erased for different session implementations.
    pub static CURRENT_SESSION: Weak<dyn MinSession>
}
