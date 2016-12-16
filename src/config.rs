extern crate serde_derive;
extern crate serde_yaml;

use std::collections::HashMap;
use std::io;
use std::io::prelude::*;
use std::fs::File;

#[derive(Serialize, Deserialize, Debug)]
pub struct ClusterConfig {
    broker_list: Vec<String>,
    zookeeper: String,
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
}

#[derive(Debug)]
pub enum ConfigError {
    IO(io::Error),
    Yaml(serde_yaml::Error),
}

impl From<io::Error> for ConfigError {
    fn from(err: io::Error) -> ConfigError {
        ConfigError::IO(err)
    }
}

impl From<serde_yaml::Error> for ConfigError {
    fn from(err: serde_yaml::Error) -> ConfigError {
        ConfigError::Yaml(err)
    }
}

pub fn read_config(path: &str) -> Result<Config, ConfigError> {
    let mut f = try!(File::open(path));
    let mut s = String::new();
    try!(f.read_to_string(&mut s));

    let config: Config = try!(serde_yaml::from_str(&s));
    Ok(config)
}
