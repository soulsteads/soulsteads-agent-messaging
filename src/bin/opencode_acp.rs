#[tokio::main]
async fn main() {
    if let Err(error) = soulsteads_messaging::adapters::opencode::run().await {
        eprintln!("OpenCode ACP adapter failed: {error}");
        std::process::exit(1);
    }
}
