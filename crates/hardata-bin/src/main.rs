#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() {
    if let Err(error) = hardata_terminal::run().await {
        eprintln!("hardata failed: {error}");
        std::process::exit(1);
    }
}
