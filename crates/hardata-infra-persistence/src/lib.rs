pub mod adapters {
    pub mod outbound {
        pub mod persistence;
    }
}

pub use adapters::outbound::persistence::db;
pub use db::{ApiIdempotencyRecord, Database, JobRetry};
