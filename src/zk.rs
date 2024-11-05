use rocket::data;
use serde_json;
use tokio_zookeeper::{WatchedEvent, ZooKeeper};

use crate::error::*;
use crate::metadata::Reassignment;

use std::str;
use std::time::Duration;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
const REASSIGN_PARTITIONS: &str = "/admin/reassign_partitions";

pub struct ZK {
    client: ZooKeeper,
}

struct NullWatcher;
impl ZK {
    pub async fn new(url: &str) -> Result<ZK> {
        info!("zk url={}",&url);
        ZooKeeper::connect(&SocketAddr::from_str(url).unwrap()).await
            .map(|client| ZK { client:client.0 })
            .chain_err(|| "Unable to connect to Zookeeper") // TODO: show url?
    }

    pub async fn pending_reassignment(&self) -> Option<Reassignment> {
        let data = match self.client.get_data(REASSIGN_PARTITIONS).await {
            Ok(Some((data, _))) => data,
            Ok(None) => {return None;}
            Err(error) => {
                println!("Error fetching reassignment: {:?}", error);
                return None;
            }
        };

        let raw = str::from_utf8(&data).ok()?;
        serde_json::from_str(raw).ok()
    }
}
