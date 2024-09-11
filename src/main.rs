use csv::Reader;
use csv::StringRecord;
use std::path::Path;
use std::time::Instant;
use tokio::fs;
use futures::future::try_join_all;

mod repo_stats;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let token = std::env::var("GITHUB_TOKEN").expect("GITHUB_TOKEN env variable is required");

    let file_path = Path::new("mydata/data.csv");
    let file_content = fs::read_to_string(file_path).await?;
    let mut reader = Reader::from_reader(file_content.as_bytes());

    if !Path::new("./repos").exists() {
        fs::create_dir("./repos").await?;
    }
    
    let headers = reader.headers()?.clone();
    println!("Header: {:?}", headers);
    
    let records: Vec<StringRecord> = reader.records().take(5).filter_map(Result::ok).collect();
    
    let start = Instant::now();
    
    let tasks: Vec<_> = records.iter().map(|record| {
        let record = record.clone();
        tokio::spawn(async move {
            println!("Record: {:?}", record);
            repo_stats::get_stats(&record).await
        })
    }).collect();

    let results = try_join_all(tasks).await?;
    let total_lines: u64 = results.into_iter().sum::<Result<u64, _>>()?;
    println!("Total lines across all repositories: {}", total_lines);

    let end = Instant::now();
    let duration = (end - start).as_secs_f32();
    println!("Duration: {duration} secs");

    Ok(())
}
