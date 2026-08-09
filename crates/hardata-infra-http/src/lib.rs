pub mod adapters {
    pub mod inbound {
        pub mod http;
    }
}

pub use adapters::inbound::http::{create_sync_router, SyncApiState};
