use clap::Parser;
use remotefs::cli::{Cli, render_error, run};

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = if error.exit_code() == 0 { 0 } else { 1 };
            let _ = error.print();
            std::process::exit(exit_code);
        }
    };
    let json = cli.json_output();

    if let Err(error) = run(cli) {
        eprintln!("{}", render_error(&error, json));
        std::process::exit(1);
    }
}
