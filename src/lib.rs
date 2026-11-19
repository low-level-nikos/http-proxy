//! This offers a very simple http-proxy that deems as learning experience rather than to be used in productive systems.

mod config;
mod header_util;
mod stream_util;
mod consumable_buffer;

/// Common errors.
pub mod error;
