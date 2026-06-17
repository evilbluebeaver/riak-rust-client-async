# riak-rust-client

[![docs.rs](https://img.shields.io/docsrs/riak-async)](https://docs.rs/riak-async)
[![Crates.io Version](https://img.shields.io/crates/v/riak-async)](https://crates.io/crates/riak-async)
[![License](https://img.shields.io/badge/license-mit-blue.svg)](LICENSE)


Async Rust client for [Riak KV](https://github.com/basho/riak) using Riak's Protocol Buffers API.

## Source and lineage

This crate is a continuation of the original work from the previous crate/repository:

- https://github.com/shaneutt/riak-rust-client

That project is the source and foundation for this codebase.

## What this client provides

- Async client built on `tokio`
- Riak KV operations for objects, buckets, bucket types, preflists, indexes, and Yokozuna
- Optional connection pooling via `deadpool` through `ClientManager` and `ClientPool`

## Requirements

- Modern stable Rust
- Riak KV 2.x with Protocol Buffers enabled (default port `8087`)

## Installation

```toml
[dependencies]
riak-async = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Quick start

```rust
use riak_async::Client;
use riak_async::object::{FetchObjectReq, ObjectContent, StoreObjectReq};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let mut riak = Client::new("127.0.0.1:8087").await?;
	riak.ping().await?;

	let mut store = StoreObjectReq::new("testbucket", ObjectContent::new("hello riak"));
	store.set_key("example-key");
	riak.store_object(store).await?;

	let fetch = FetchObjectReq::new("testbucket", "example-key");
	let resp = riak.fetch_object(fetch).await?;

	if let Some(content) = resp.content.first() {
		println!("value={}", String::from_utf8_lossy(content.get_value()));
	}

	Ok(())
}
```

## Pool examples

### 1) Single-node pool

```rust
use riak_async::{ClientManager, ClientPool};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let manager = ClientManager::new(&["127.0.0.1:8087"])?;
	let pool = ClientPool::builder(manager).max_size(16).build()?;

	let mut client = pool.get().await?;
	client.ping().await?;

	Ok(())
}
```

### 2) Multi-node pool with round-robin creation

```rust
use riak_async::{ClientManager, ClientPool};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let manager = ClientManager::new(&[
		"10.0.0.10:8087",
		"10.0.0.11:8087",
		"10.0.0.12:8087",
	])?;

	// New pooled clients are created against addresses in round-robin order.
	let pool = ClientPool::builder(manager).max_size(32).build()?;

	let mut client = pool.get().await?;
	let (node, version) = client.server_info().await?;
	println!("connected to {} ({})", node, version);

	Ok(())
}
```

### 3) Reusing pooled clients for write/read operations

```rust
use riak_async::{ClientManager, ClientPool};
use riak_async::object::{FetchObjectReq, ObjectContent, StoreObjectReq};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let manager = ClientManager::new(&["127.0.0.1:8087"])?;
	let pool = ClientPool::builder(manager).max_size(8).build()?;

	{
		let mut client = pool.get().await?;
		let mut store = StoreObjectReq::new("pool-bucket", ObjectContent::new("pool-data"));
		store.set_key("pool-key");
		client.store_object(store).await?;
	} // client is returned to the pool here

	{
		let mut client = pool.get().await?;
		let fetch = FetchObjectReq::new("pool-bucket", "pool-key");
		let resp = client.fetch_object(fetch).await?;
		assert!(!resp.content.is_empty());
	}

	Ok(())
}
```

## Notes

- If a request stream is interrupted and leaves unread response data, that client can be marked broken.
- The pool manager checks this during recycle and reconnects before reusing the client.

## Contributing

Issues and pull requests are welcome.
