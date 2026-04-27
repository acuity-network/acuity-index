//! WebSocket API server.

mod listener;
mod requests;
mod session;
#[cfg(test)]
mod tests_support;
mod validation;

use crate::errors::IndexError;

pub use listener::websockets_listen;
pub use requests::process_msg_status;

fn disconnect_error(message: impl Into<String>) -> IndexError {
    IndexError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionAborted,
        message.into(),
    ))
}
