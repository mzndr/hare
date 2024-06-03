//! [`signal_stop`] utility function.

use tokio::io;
use tokio::signal::unix::{self, SignalKind};
use tracing::{info, instrument};

/// Creates listeners for the `SIGINT`/`SIGTERM` signals and waits for either of them to be sent.
///
/// # Errors
///
/// This function may return an error if the listeners for the `SIGINT`/`SIGTERM` signals can not
/// be created.
#[instrument]
pub async fn signal_stop() -> Result<(), io::Error> {
    let mut sigint = unix::signal(SignalKind::interrupt())?;
    let mut sigterm = unix::signal(SignalKind::terminate())?;

    tokio::select! {
        _ = sigint.recv() => info!("received SIGINT signal"),
        _ = sigterm.recv() => info!("received SIGTERM signal"),
    }

    Ok(())
}
