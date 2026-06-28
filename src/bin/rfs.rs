use clap::Parser;
use remotefs::cli::{Cli, render_error_for_command, run};

#[tokio::main]
async fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = if error.exit_code() == 0 { 0 } else { 1 };
            let _ = error.print();
            std::process::exit(exit_code);
        }
    };
    let json = cli.json_output();
    let command = cli.command_name();

    if let Err(error) = run(cli).await {
        eprintln!("{}", render_error_for_command(&error, json, command));
        std::process::exit(1);
    }
}
