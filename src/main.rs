use clap::Parser;
use core::str;
use error_chain::error_chain;
use std::{
    fs::{self, File},
    io::{BufRead, Write},
    path::PathBuf,
    process::Command,
};

const DATASETS_DIR: &str = "./datasets";
const IMDB_DATASETS: [&str; 7] = [
    "https://datasets.imdbws.com/name.basics.tsv.gz",
    "https://datasets.imdbws.com/title.basics.tsv.gz",
    "https://datasets.imdbws.com/title.ratings.tsv.gz",
    "https://datasets.imdbws.com/title.crew.tsv.gz",
    "https://datasets.imdbws.com/title.principals.tsv.gz",
    "https://datasets.imdbws.com/title.episode.tsv.gz",
    "https://datasets.imdbws.com/title.akas.tsv.gz",
];

error_chain! {
    foreign_links {
        Io(std::io::Error);
        HttpRequest(reqwest::Error);
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    pg_connection_string: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!(
        r"
░▀█▀░█▄█░█▀▄░█▀▄░░░░█▀▄░█▀▀
░░█░░█░█░█░█░█▀▄░░░░█▀▄░▀▀█
░▀▀▀░▀░▀░▀▀░░▀▀░░▀░░▀░▀░▀▀▀
    "
    );
    println!(r"🚀 Downloading IMDB datasets and importing them into PostgreSQL...");

    fs::create_dir_all(DATASETS_DIR)?;
    let temp_dir = PathBuf::from(DATASETS_DIR);

    for file_url in IMDB_DATASETS.iter() {
        let file = {
            let file = download_file(file_url, &temp_dir).await?;
            Command::new("gunzip").arg("-f").arg(&file).output()?;
            file.replace(".gz", "")
        };

        let table_name = file
            .split("/")
            .last()
            .unwrap()
            .replace(".tsv", "")
            .replace(".", "_");
        let table_columns = get_table_columns_from_tsv(&file)?;
        let create_table_query = get_create_table_query(&table_name, &table_columns);

        let copy_to_db_command_output = Command::new("psql")
            .arg(&args.pg_connection_string)
            .arg("-c")
            .arg(create_table_query)
            .arg("-c")
            .arg(format!(
                r"\COPY {} FROM '{}' WITH DELIMITER E'\t' QUOTE E'\b' NULL AS '\N' CSV HEADER",
                table_name, file
            ))
            .output()?;

        println!(
            "✅ Done: {} - {}",
            table_name,
            str::from_utf8(&copy_to_db_command_output.stdout)
                .unwrap()
                .trim()
                .split("\n")
                .last()
                .unwrap()
                .to_lowercase()
        );
    }

    fs::remove_dir_all(DATASETS_DIR)?;

    Ok(())
}

async fn download_file(url: &str, dest_path: &PathBuf) -> Result<String> {
    let response = reqwest::get(url).await?;
    let file_name = {
        let fname = response
            .url()
            .path_segments()
            .and_then(|segments| segments.last())
            .and_then(|name| if name.is_empty() { None } else { Some(name) })
            .unwrap_or("tmp.bin");
        dest_path.join(fname)
    };
    let mut file = File::create(&file_name)?;
    let bytes = response.bytes().await?;
    file.write(&bytes)?;
    Ok(file_name.to_str().unwrap().to_string())
}

fn get_table_columns_from_tsv(file: &str) -> Result<String> {
    let file = File::open(file)?;
    let reader = std::io::BufReader::new(file);
    let header = reader.lines().next().unwrap()?;
    Ok(header.split("\t").collect::<Vec<&str>>().join(","))
}

fn get_create_table_query(table_name: &str, table_columns: &String) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {table_name} ({}); TRUNCATE {table_name};",
        table_columns
            .split(",")
            .map(|c| format!("{} TEXT", c))
            .collect::<Vec<String>>()
            .join(",")
    )
}
