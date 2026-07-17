use clap::Parser;
use rfsd::{Cli, run};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(error) = run(cli).await {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}
