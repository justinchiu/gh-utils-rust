use std::time::Instant;
use std::sync::Arc;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ObjectStore, path::Path};
use futures::stream::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use bytes::Bytes;
use arrow::array::StringArray;
use arrow::record_batch::RecordBatchReader;
use indicatif::{ProgressBar, ProgressStyle, ParallelProgressIterator};
use rayon::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket_name = "cohere-data";
    let prefix = "dataacq/github-repos/permissive_and_unlicensed/repo-level-rows/";

    let start = Instant::now();

    let store = Arc::new(GoogleCloudStorageBuilder::new()
        .with_bucket_name(bucket_name)
        .build()?);

    let list_stream = store.list(Some(&Path::from(prefix)));
    
    let objects: Vec<_> = list_stream
        .filter_map(|meta| async {
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

    let all_results: Vec<_> = objects.par_iter()
        .progress_with(progress_bar)
        .flat_map(|path| {
            let store = Arc::clone(&store);
            let result = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async {
                    match store.get(path).await {
                        Ok(object) => {
                            let data = object.bytes().await.ok()?;
                            let size_gb = data.len() as f64 / 1_073_741_824.0; // Convert bytes to GB
                            
                            match process_parquet(&data) {
                                Ok(results) => Some(results),
                                Err(e) => {
                                    eprintln!("Error processing {:?}: {:?}", path, e);
                                    None
                                }
                            }
                        },
                        Err(e) => {
                            eprintln!("Error fetching object {:?}: {:?}", path, e);
                            None
                        }
                    }
                });
            result.unwrap_or_else(Vec::new)
        })
        .flatten()
        .collect();

    println!("Total matching rows across all Parquet files: {}", all_results.len());

    let end = Instant::now();
    let duration = (end - start).as_secs_f32();
    println!("Duration: {duration} secs");

    Ok(())
}

fn process_parquet(data: &[u8]) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let bytes = Bytes::from(data.to_vec());
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let reader = builder.build()?;

    let _schema = reader.schema().clone();
    let mut results = Vec::new();

    for batch in reader {
        let batch = batch?;
        if let Some(column) = batch.column(0).as_any().downcast_ref::<StringArray>() {
            for str_value in column.iter().flatten() {
                // Add your filtering logic here
                if str_value.contains("your_filter_condition") {
                    results.push(str_value.to_string());
                }
            }
        }
    }

    Ok(results)
}
