use serde_yaml;

use crate::error::*;
use crate::metadata::ClusterId;

use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;

fn default_true() -> bool {
    true
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClusterConfig {
    pub cluster_id: Option<ClusterId>, // This will always be available after load
    pub broker_list: Vec<String>,
    pub zookeeper: String,
    pub jolokia_port: Option<i32>,
    pub graph_url: Option<String>,
    #[serde(default = "default_true")]
    pub enable_tailing: bool,
    #[serde(default = "default_true")]
    pub show_zk_reassignments: bool,
    pub security_protocol:Option<String>,
    pub sasl_kerberos_service_name:Option<String>,
    pub sasl_mechanism:Option<String>,
    pub sasl_kerberos_principal:Option<String>,
    pub sasl_kerberos_keytab:Option<String>,
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
    pub offsets_store_duration: u64,
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
    let mut f = File::open(path).chain_err(|| "Unable to open configuration file")?;
    let mut s = String::new();
    f.read_to_string(&mut s)
        .chain_err(|| "Unable to read configuration file")?;

    let mut config: Config =
        serde_yaml::from_str(&s).chain_err(|| "Unable to parse configuration file")?;

    for (cluster_id, cluster) in &mut config.clusters {
        cluster.cluster_id = Some(cluster_id.clone());
    }

    info!("Configuration: {:?}", config);

    Ok(config)
}
