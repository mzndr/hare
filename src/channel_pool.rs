//! [`ChannelPool`] definition and implementation.
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use deadpool::managed;

/// Wrap [`managed::Pool`] for channels and add convenience functions
/// to interface with it.
#[derive(Debug, Clone)]
pub(super) struct ChannelPool(managed::Pool<ChannelManager>);

impl ChannelPool {
    /// Create a new pool.
    ///
    /// # Errors
    /// See [`NewError`]
    pub fn new(conn: Arc<lapin::Connection>) -> Result<Self, NewError> {
        let inner = managed::Pool::builder(ChannelManager(conn))
            .build()
            .map_err(|err| NewError::Other(err.into()))?;
        Ok(Self(inner))
    }

    /// Get a [`Channel`].
    ///
    /// # Errors
    /// See [`GetError`]
    pub async fn get(&self) -> Result<Channel, GetError> {
        let inner = self
            .0
            .get()
            .await
            .map_err(|err| GetError::Other(err.into()))?;
        Ok(Channel(inner))
    }
}

/// Errors that can occur when creating a new pool.
#[derive(Debug, thiserror::Error)]
pub enum NewError {
    /// Any errors.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Errors that can occur when getting a channel.
#[derive(Debug, thiserror::Error)]
pub enum GetError {
    /// Any errors.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Deref for ChannelPool {
    type Target = managed::Pool<ChannelManager>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Wrap [`lapin::Connection`] to implement [`managed::Manager`] for it.
#[derive(Debug, Clone)]
pub struct ChannelManager(Arc<lapin::Connection>);

#[async_trait]
impl managed::Manager for ChannelManager {
    type Type = lapin::Channel;
    type Error = lapin::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        self.0.create_channel().await
    }

    async fn recycle(
        &self,
        chan: &mut lapin::Channel,
    ) -> Result<(), managed::RecycleError<Self::Error>> {
        if let lapin::ChannelState::Connected = chan.status().state() {
            Ok(())
        } else {
            Err(managed::RecycleError::StaticMessage(
                "channel is not connected",
            ))
        }
    }
}

/// Wrap the channel Manager to be managed by deadpool.
#[derive(Debug)]
pub struct Channel(managed::Object<ChannelManager>);

impl Deref for Channel {
    type Target = managed::Object<ChannelManager>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
