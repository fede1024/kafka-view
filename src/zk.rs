use serde_json;
use zookeeper::{WatchedEvent, Watcher, ZkError, ZooKeeper};

use error::*;
use metadata::Reassignment;

use std::str;
use std::time::Duration;

const REASSIGN_PARTITIONS: &str = "/admin/reassign_partitions";

pub struct ZK {
    client: ZooKeeper,
}

struct NullWatcher;
impl Watcher for NullWatcher {
    fn handle(&self, _: WatchedEvent) {}
}

impl ZK {
    pub fn new(url: &str) -> Result<ZK> {
        ZooKeeper::connect(url, Duration::from_secs(15), NullWatcher)
            .map(|client| ZK { client })
            .chain_err(|| "Unable to connect to Zookeeper") // TODO: show url?
    }

    pub fn pending_reassignment(&self) -> Option<Reassignment> {
        let data = match self.client.get_data(REASSIGN_PARTITIONS, false) {
            Ok((data, _)) => data,
            Err(ZkError::NoNode) => return None, // no pending reassignment node
            Err(error) => {
                println!("Error fetching reassignment: {:?}", error);
                return None;
            }
        };

        let raw = str::from_utf8(&data).ok()?;
        serde_json::from_str(raw).ok()
    }
}
