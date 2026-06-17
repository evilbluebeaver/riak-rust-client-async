extern crate riak_async;

use std::io::ErrorKind;

use riak_async::errors::RiakErr;
use riak_async::{ClientManager, ClientPool};

#[test]
fn test_client_manager_rejects_empty_addresses() {
    let err = ClientManager::<&str>::new(&[]).expect_err("expected empty address list to fail");

    match err {
        RiakErr::IoError(io_err) => assert_eq!(io_err.kind(), ErrorKind::InvalidInput),
        other => panic!("unexpected error type: {other:?}"),
    }
}

#[test]
fn test_explicit_pool_creation() {
    let manager = ClientManager::new(&["127.0.0.1:8087"]).expect("manager creation should succeed");
    let pool = ClientPool::builder(manager)
        .max_size(2)
        .build()
        .expect("pool builder should succeed");

    drop(pool);
}
