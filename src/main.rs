use std::time::Instant;
use std::sync::Arc;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ObjectStore, path::Path};
use parquet::file::reader::{FileReader, SerializedFileReader};
use object_store::GetResult;
use indicatif::{ProgressBar, ProgressStyle};
use futures::StreamExt;
use bytes::Bytes;

#[derive(Default,Debug)]
struct Info {
    repo: String,
    num_lines: usize,
    num_files: usize,
    has_tests: bool,
    has_docs: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket_name = "cohere-data";
    let prefix = "dataacq/github-repos/permissive_and_unlicensed/repo-level-rows/";

    let start = Instant::now();

    let store = Arc::new(GoogleCloudStorageBuilder::new()
        .with_bucket_name(bucket_name)
        .build()?);

    let objects: Vec<_> = store.list(Some(&Path::from(prefix)))
        .filter_map(|meta| async move {
            match meta {
                Ok(meta) if meta.location.as_ref().ends_with(".parquet") => Some(meta.location),
                _ => None,
            }
        })
        .collect()
        .await;

    println!("Number of parquet files: {}", objects.len());

    let progress_bar = ProgressBar::new(objects.len() as u64);
    progress_bar.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("##-"));

    let mut all_results: Vec<Info> = Vec::new();
    for path in objects.iter() {
        progress_bar.inc(1);
        let result: GetResult = store.get(path).await.unwrap();
        println!("Got result");
        let data = result.bytes().await?;
        println!("Got bytes");
        let size_gb = data.len() as f64 / 1_073_741_824.0; // Convert bytes to GB
        match process_parquet(data) {
            Ok(mut results) => all_results.append(&mut results),
            Err(e) => eprintln!("Error processing {:?}: {:?}", path, e),
        };
        println!("yay!");
    }
    progress_bar.finish();

    println!("Total processed repos: {}", all_results.len());

    let end = Instant::now();
    let duration = (end - start).as_secs_f32();
    println!("Duration: {duration} secs");

    Ok(())
}

fn process_parquet(data: Bytes) -> Result<Vec<Info>, Box<dyn std::error::Error>> {
    let reader = SerializedFileReader::new(data)?;
    let mut iter = reader.get_row_iter(None)?;

    let mut results = Vec::new();

    println!("Number of row groups: {}", reader.num_row_groups());

    Ok(results)
}
