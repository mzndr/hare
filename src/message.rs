//! Message [`Payload`] definition and implementation.
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Message id.
pub type Id = Uuid;

/// Message payload.
#[derive(Debug, Clone)]
pub struct Payload<T>(pub T);

impl<'de, T> Payload<T>
where
    T: Deserialize<'de>,
{
    /// Deserializes a message payload of type `T` from the given bytes.
    ///
    /// # Errors
    ///
    /// Possible errors originate from `serde_json`.
    pub fn deserialize(bytes: &'de [u8]) -> Result<Self, DeserializeError> {
        let inner = serde_json::from_slice(bytes).map_err(|err| DeserializeError(err.into()))?;
        Ok(Self(inner))
    }
}

/// When deserializing message payload fails.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct DeserializeError(#[from] anyhow::Error);

impl<T> Payload<T>
where
    T: Serialize,
{
    /// Serializes the message payload to a vector of bytes.
    ///
    /// # Errors
    ///
    /// Possible errors originate from `serde_json`.
    pub fn serialize(&self) -> Result<Vec<u8>, SerializeError> {
        serde_json::to_vec(&self.0).map_err(|err| SerializeError(err.into()))
    }
}

/// When serializing message payload fails.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct SerializeError(#[from] anyhow::Error);

impl<T> Deref for Payload<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Payload<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
