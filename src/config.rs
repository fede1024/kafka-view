extern crate serde_yaml;

use error::*;

use std::collections::HashMap;
use std::io::prelude::*;
use std::fs::File;

#[derive(Serialize, Deserialize, Debug)]
pub struct ClusterConfig {
    broker_list: Vec<String>,
    zookeeper: String,
}

impl ClusterConfig {
    pub fn broker_list(&self) -> &Vec<String> {
        &self.broker_list
    }

    pub fn broker_string(&self) -> String {
        self.broker_list.join(",")
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CachingConfig {
    pub cluster: String,
    pub topic: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub metadata_refresh: u64,
    pub metrics_refresh: u64,
    pub clusters: HashMap<String, ClusterConfig>,
    pub caching: CachingConfig,
}

impl Config {
    pub fn cluster(&self, cluster_name: &str) -> Option<&ClusterConfig> {
        self.clusters.get(cluster_name)
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
