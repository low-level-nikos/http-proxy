//! This offers a very simple http-proxy that deems as learning experience rather than to be used in productive systems.

mod config;
mod consumable_buffer;
mod header_util;
mod proxy;
mod stream_util;

pub mod server;

/// Common errors.
pub mod error;
