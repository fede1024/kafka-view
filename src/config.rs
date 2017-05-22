use serde_yaml;

use metadata::ClusterId;
use error::*;

use std::collections::HashMap;
use std::io::prelude::*;
use std::fs::File;


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClusterConfig {
    pub broker_list: Vec<String>,
    pub zookeeper: String,
    pub jolokia_port: Option<i32>,
    pub graph_url: Option<String>,
}

impl ClusterConfig {
    pub fn bootstrap_servers(&self) -> String {
        self.broker_list.join(",")
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CachingConfig {
    pub cluster: ClusterId,
    pub topic: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub listen_port: u16,
    pub listen_host: String,
    pub metadata_refresh: u64,
    pub metrics_refresh: u64,
    pub consumer_offsets_group_id: String,
    pub clusters: HashMap<ClusterId, ClusterConfig>,
    pub caching: CachingConfig,
}

impl Config {
    pub fn cluster(&self, cluster_id: &ClusterId) -> Option<&ClusterConfig> {
        self.clusters.get(cluster_id)
    }
}

pub fn read_config(path: &str) -> Result<Config> {
    let mut f = File::open(path)
        .chain_err(|| "Unable to open configuration file")?;;
    let mut s = String::new();
    f.read_to_string(&mut s)
        .chain_err(|| "Unable to read configuration file")?;

    let config: Config = serde_yaml::from_str(&s)
        .chain_err(|| "Unable to parse configuration file")?;

    Ok(config)
}
