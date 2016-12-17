extern crate serde_yaml;

use error::*;

use std::collections::HashMap;
use std::io;
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
pub struct Config {
    clusters: HashMap<String, ClusterConfig>
}

impl Config {
    fn new(clusters: HashMap<String, ClusterConfig>) -> Config {
        Config {
            clusters: clusters
        }
    }

    pub fn clusters(&self) -> &HashMap<String, ClusterConfig> {
        &self.clusters
    }

    pub fn cluster(&self, cluster_name: &str) -> Option<&ClusterConfig> {
        self.clusters.get(cluster_name)
    }
}

// #[derive(Debug)]
// pub enum ConfigError {
//     IO(io::Error),
//     Yaml(serde_yaml::Error),
// }
//
// impl From<io::Error> for ConfigError {
//     fn from(err: io::Error) -> ConfigError {
//         ConfigError::IO(err)
//     }
// }
//
// impl From<serde_yaml::Error> for ConfigError {
//     fn from(err: serde_yaml::Error) -> ConfigError {
//         ConfigError::Yaml(err)
//     }
// }

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
