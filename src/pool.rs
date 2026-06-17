use std::io::{self, ErrorKind};
use std::sync::atomic::{AtomicUsize, Ordering};

use deadpool::managed::{Manager, Metrics, Object, Pool, RecycleResult};
use tokio::net::ToSocketAddrs;

use crate::Client;
use crate::errors::RiakErr;

/// Deadpool manager that creates and recycles `Client` connections.
#[derive(Debug)]
pub struct ClientManager<A> {
    addrs: Vec<A>,
    next_addr: AtomicUsize,
}

impl<A> ClientManager<A>
where
    A: ToSocketAddrs + Clone + Send + Sync + 'static,
{
    /// Create a manager for pooling `Client` connections.
    pub fn new(addrs: &[A]) -> Result<Self, RiakErr> {
        if addrs.is_empty() {
            return Err(RiakErr::IoError(io::Error::new(
                ErrorKind::InvalidInput,
                "pool requires at least one address",
            )));
        }

        Ok(Self {
            addrs: addrs.to_vec(),
            next_addr: AtomicUsize::new(0),
        })
    }
}

impl<A> Manager for ClientManager<A>
where
    A: ToSocketAddrs + Clone + Send + Sync + 'static,
{
    type Type = Client;
    type Error = RiakErr;

    async fn create(&self) -> Result<Client, RiakErr> {
        let idx = self.next_addr.fetch_add(1, Ordering::Relaxed) % self.addrs.len();
        Client::new(self.addrs[idx].clone()).await
    }

    async fn recycle(&self, obj: &mut Client, _metrics: &Metrics) -> RecycleResult<RiakErr> {
        // If a request future was dropped mid-flight, reconnect before reuse.
        if obj.is_connection_broken() {
            obj.reconnect().await?;
        }

        Ok(())
    }
}

/// Pool type for reusable `Client` connections.
pub type ClientPool<A> = Pool<ClientManager<A>>;

/// Pooled `Client` object returned from `ClientPool::get`.
pub type PooledClient<A> = Object<ClientManager<A>>;
