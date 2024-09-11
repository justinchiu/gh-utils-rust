use csv::{Reader, StringRecord, WriterBuilder};
use std::path::Path;
use std::time::Instant;
use tokio::fs;
use futures::future::try_join_all;
use std::env;
use std::fs::File;

mod repo_stats;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file_path = Path::new("mydata/data.csv");
    if !file_path.exists() {
        eprintln!("Error: Input file 'mydata/data.csv' not found.");
        std::process::exit(1);
    }

    let file_content = fs::read_to_string(file_path).await?;
    let mut reader = Reader::from_reader(file_content.as_bytes());

    if !Path::new("./repos").exists() {
        fs::create_dir("./repos").await?;
    }
    
    let mut headers = reader.headers()?.clone();
    headers.push_field("total_lines");
    headers.push_field("comment_lines");
    println!("Header: {:?}", headers);
    
    let records: Vec<StringRecord> = reader.records().take(10).filter_map(Result::ok).collect();
    
    let start = Instant::now();
    
    let github_token = env::var("GITHUB_TOKEN").unwrap_or_else(|_| {
        eprintln!("Error: GITHUB_TOKEN environment variable is not set.");
        eprintln!("Please set it with: export GITHUB_TOKEN=your_token_here");
        std::process::exit(1);
    });

    let tasks: Vec<_> = records.iter().map(|record| {
        let record = record.clone();
        let token = github_token.clone();
        tokio::spawn(async move {
            println!("Record: {:?}", record);
            repo_stats::get_stats(&record, Some(&token)).await
        })
    }).collect();

    let results = try_join_all(tasks).await?;
    
    write_csv_results(&headers, &records, &results)?;

    let end = Instant::now();
    let duration = (end - start).as_secs_f32();
    println!("Duration: {duration} secs");

    Ok(())
}

fn write_csv_results(
    headers: &StringRecord,
    records: &[StringRecord],
    results: &[Result<(u64, u64, u64), Box<dyn std::error::Error + Send + Sync>>],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = File::create("mydata/new_data.csv")?;
    let mut writer = WriterBuilder::new().from_writer(file);
    writer.write_record(headers)?;

    for (i, (record, result)) in records.iter().zip(results.iter()).enumerate() {
        if let Ok((total_lines, comment_lines, _)) = result {
            println!("Repository {}: Total Lines: {}, Comment Lines: {}", i + 1, total_lines, comment_lines);
            let mut new_record = record.clone();
            new_record.push_field(&total_lines.to_string());
            new_record.push_field(&comment_lines.to_string());
            writer.write_record(&new_record)?;
        }
    }

    writer.flush()?;
    Ok(())
}
