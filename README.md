# http_proxy

A minimal asynchronous HTTP `CONNECT` proxy written in Rust (Edition 2024) using `tokio`.

> **Disclaimer:** This project is built as a learning exercise and is **not** intended for production use. It does not implement the full HTTP/1.1 or proxy RFC specifications and only supports tunneling via the `CONNECT` method.

## Overview

`http_proxy` binds a TCP listener on a configurable host and port, spawns an async task per client connection, parses an incoming `CONNECT <host>:<port> HTTP/1.1` request header, opens a TCP connection to the target, responds with `200 Connection Established`, and relays bytes bidirectionally with read and write timeouts.

### Project Structure

| File | Responsibility |
| :--- | :--- |
| `src/main.rs` | CLI entry point powered by `clap` (`--host` and `--port`). |
| `src/lib.rs` | Crate root declaring internal modules and exposing `server` and `error`. |
| `src/server.rs` | `TcpListener` loop that accepts connections and spawns `proxy::handle_stream`. |
| `src/proxy.rs` | Handles handshake reading/parsing, target connection, `200` response, and stream relaying. |
| `src/header_util.rs` | Extracts headers up to `\r\n\r\n` with header-size protection and validates `CONNECT` requests. |
| `src/consumable_buffer.rs` | Fixed-capacity `[u8; N]` sliding buffer (`ConsumableBuffer`) with byte consumption and flushing. |
| `src/stream_util.rs` | Bidirectional stream forwarding (`redirect_streams` / `forward`) with read and write timeouts. |
| `src/error.rs` | `ProxyError` definitions (`thiserror`) and HTTP error responses (`400 Bad Request` / `500 Internal Server Error`). |
| `src/config.rs` | Compile-time constants for buffer sizes and read/write/connection timeouts. |

## Prerequisites & Dependencies

- **Rust** (Edition `2024`)
- Key crate dependencies (`Cargo.toml`):
  - `tokio` (`full`) — async runtime, TCP networking, timers, and `select!`
  - `clap` (`derive`) — CLI argument parsing
  - `regex` — matching `CONNECT <target> HTTP/1.1` request lines
  - `http` — HTTP status codes
  - `thiserror` — ergonomic error types

## Getting Started

### Build & Run

```bash
cargo run -- --host 127.0.0.1 --port 20001

```

#### CLI Options

* `--host <HOST>` — Host address to bind the proxy listener to (e.g., `127.0.0.1` or `0.0.0.0`).
* `--port <PORT>` — TCP port (`u16`) to listen on.

### Example
```bash
curl -v -x http://127.0.0.1:20001 https://example.com
```

### Running Tests

The codebase includes unit and integration tests covering `CONNECT` tunneling, non-`CONNECT` rejection, malformed/oversized/invalid-UTF-8 headers, buffer operations, and read/write timeouts:

```bash
cargo test

```

### Configuration

Compile-time settings are defined as constants in `src/config.rs` (changing them requires recompiling the crate):

| Constant | Default Value | Purpose |
| --- | --- | --- |
| `MAX_HEADER_BYTE_COUNT` | `8192` | Maximum allowed header size in bytes before returning `400 Bad Request`. |
| `CONNECTION_TIMEOUT_SECS` | `2` | Timeout (in seconds) when connecting to the target server. |
| `READ_TIMEOUT_SECS` | `2` | Timeout (in seconds) for reading headers and forwarding stream chunks. |
| `WRITE_TIMEOUT_SECS` | `2` | Timeout (in seconds) when writing buffered data to a stream. |
| `BUFFER_SIZE` | `8192` | Default capacity (`N`) for `ConsumableBuffer` instances. |


## Architecture & Design Notes

* **Bounded Memory & Header-Flood Protection:** Rather than relying on unbounded buffers (such as an unrestricted `BytesMut`), `ConsumableBuffer<const N: usize>` wraps a fixed stack/inline `[u8; N]` array with `write_pos` and `consume_pos` cursors and `copy_within` sliding compaction when unconsumed bytes remain. During handshake parsing (`extract_header`), bytes are consumed incrementally until `\r\n\r\n` is matched, enforcing `MAX_HEADER_BYTE_COUNT` while leaving any trailing payload bytes in the buffer for the tunnel phase.
* **Explicit Timeouts & Concurrent Tunneling:** Every I/O stage is bounded by `tokio::time::timeout`—header extraction (`READ_TIMEOUT_SECS`), outbound target connection (`CONNECTION_TIMEOUT_SECS`), and chunk reads/writes (`READ_TIMEOUT_SECS` / `WRITE_TIMEOUT_SECS`)—preventing stalled peers from holding tasks open indefinitely. Bidirectional relaying (`redirect_streams`) runs both `forward` directions concurrently inside `tokio::select!`, returning as soon as either direction finishes or hits an error/timeout.
* **Typed Domain Errors & HTTP Status Mapping:** Failure modes are modeled explicitly in `ProxyError` via `thiserror`, distinguishing client-side protocol/input errors (such as non-`CONNECT` methods, malformed or non-UTF-8 headers, and oversized headers, mapped to `400 Bad Request`) from handshake/network/timeout/internal errors (mapped to `500 Internal Server Error` with a plain-text body and `Connection: close`).

## License

This project is licensed under the **MIT License**.

