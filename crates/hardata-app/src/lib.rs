pub mod adapters;
pub mod application;

// Application orchestration and compatibility facades live here; core types are
// exposed by dedicated workspace crates.
pub mod domain {
    pub use hardata_domain::*;
    pub use hardata_protocol::*;
}

pub mod protocol {
    pub use hardata_protocol::*;
}

pub mod shared {
    pub use hardata_shared::*;
}

pub use adapters::outbound::transport::quic::QuicClient;
pub use domain::chunk::Chunk;
pub use domain::job::{Job, JobConfig, JobPath, JobStatus};
pub use shared::error::{HarDataError, Result};

// Compatibility facades for pre-Clean-Architecture module paths.
#[doc(hidden)]
#[allow(unused_imports)]
pub mod core {
    pub use crate::domain::*;
    pub use crate::domain::{chunk, job, transfer_state};
    pub use crate::protocol::*;
    pub use crate::shared::constants;
}

#[doc(hidden)]
pub mod util {
    pub use crate::shared::*;
}

#[doc(hidden)]
pub mod sync {
    pub use crate::adapters::outbound::persistence as storage;
    pub use crate::adapters::outbound::transport as net;
    pub use crate::application::config::{RegionConfig, SyncConfig};
    pub use crate::application::sync::engine;
    pub use crate::application::sync::scanner;
    pub use crate::application::sync::transfer;
}
