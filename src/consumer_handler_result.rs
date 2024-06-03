//! [`ConsumerHandlerResult`] definition and implementation.
use std::time::Duration;

/// Things that can consuming a message.
#[derive(Debug, thiserror::Error)]
pub enum ConsumerHandlerError {
    /// An extractor has failed.
    #[error("{0} extractor failed: {1}")]
    Extractor(String, anyhow::Error),
    /// The consumer has timed out.
    #[error("timeout of {0:?} reached")]
    Timeout(Duration),
    /// Something else went wrong.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Concrete [`ConsumerHandlerResult`].
pub(super) type ConsumerHandlerResult = Result<(), ConsumerHandlerError>;

/// Transform other values into a [`ConsumerHandlerResult`].
pub(super) trait IntoConsumerHandlerResult {
    /// Transform other values into a [`ConsumerHandlerResult`].
    fn into_consumer_handler_result(self) -> ConsumerHandlerResult;
}

impl IntoConsumerHandlerResult for () {
    fn into_consumer_handler_result(self) -> ConsumerHandlerResult {
        Ok(self)
    }
}

impl<E> IntoConsumerHandlerResult for Result<(), E>
where
    E: Into<anyhow::Error>,
{
    fn into_consumer_handler_result(self) -> ConsumerHandlerResult {
        self.map_err(|err| ConsumerHandlerError::Other(err.into()))
    }
}
