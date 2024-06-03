//! [`OnError`] trait definition and implementation.

/// Call provided closure if conditions are met.
pub trait OnError<E>
where
    E: std::fmt::Display,
{
    #[must_use]
    /// Call provided closure if conditions are met.
    fn on_err<F>(self, f: F) -> Self
    where
        F: FnOnce(&E);
}

impl<E> OnError<E> for E
where
    E: std::error::Error,
{
    /// Call provided closure.
    fn on_err<F>(self, f: F) -> Self
    where
        F: FnOnce(&E),
    {
        f(&self);
        self
    }
}

impl<T, E> OnError<E> for Result<T, E>
where
    E: std::fmt::Display,
{
    /// Call provided closure when [`Result`] is [`Err`].
    fn on_err<F>(self, f: F) -> Self
    where
        F: FnOnce(&E),
    {
        if let Err(err) = &self {
            f(err);
        }
        self
    }
}
