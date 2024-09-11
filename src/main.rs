use csv::{Reader, Writer};
use csv::StringRecord;
use std::path::Path;
use std::time::Instant;
use tokio::fs;
use futures::future::try_join_all;

mod repo_stats;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file_path = Path::new("mydata/data.csv");
    let file_content = fs::read_to_string(file_path).await?;
    let mut reader = Reader::from_reader(file_content.as_bytes());

    if !Path::new("./repos").exists() {
        fs::create_dir("./repos").await?;
    }
    
    let mut headers = reader.headers()?.clone();
    headers.push_field("total_lines");
    headers.push_field("comment_lines");
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
    
    let mut writer = Writer::from_path("mydata/new_data.csv")?;
    writer.write_record(&headers)?;

    for (i, (record, result)) in records.iter().zip(results.into_iter()).enumerate() {
        if let Ok((total_lines, total_comment_lines)) = result {
            println!("Repository {}: Total Lines: {}, Total Comment Lines: {}", i + 1, total_lines, total_comment_lines);
            let mut new_record = record.clone();
            new_record.push_field(&total_lines.to_string());
            new_record.push_field(&total_comment_lines.to_string());
            writer.write_record(&new_record)?;
        }
    }

    writer.flush()?;

    let end = Instant::now();
    let duration = (end - start).as_secs_f32();
    println!("Duration: {duration} secs");

    Ok(())
}
