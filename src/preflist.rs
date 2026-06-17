// Preflists are lists of VNodes responsible for a Riak object
//
// For more information: https://docs.basho.com/riak/kv/latest/learn/concepts/replication/
//

use crate::proto::RpbBucketKeyPreflistItem;

/// `PrefListItem` represents a Riak preflist for a key
#[derive(Clone, Debug)]
pub struct PreflistItem {
    pub partition: i64,
    pub node: String,
    pub is_primary: bool,
}

impl PreflistItem {
    pub fn new(partition: i64, node: &str, is_primary: bool) -> PreflistItem {
        PreflistItem {
            partition,
            node: node.to_string(),
            is_primary,
        }
    }
}

impl From<RpbBucketKeyPreflistItem> for PreflistItem {
    fn from(item: RpbBucketKeyPreflistItem) -> PreflistItem {
        PreflistItem {
            partition: item.partition,
            node: String::from_utf8_lossy(&item.node).into_owned(),
            is_primary: item.primary,
        }
    }
}
