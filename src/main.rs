use std::time::Instant;
use std::sync::Arc;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ObjectStore, path::Path};
use futures::stream::{self, StreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use bytes::Bytes;
use arrow::array::StringArray;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatchReader;
use indicatif::{ProgressBar, ProgressStyle};

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

    let objects = stream::iter(objects)
        .map(|path| {
            let store = Arc::clone(&store);
            let pb = progress_bar.clone();
            async move {
                let result = match store.get(&path).await {
                    Ok(object) => {
                        let data = object.bytes().await.ok()?;
                        Some((path, data))
                    },
                    Err(e) => {
                        eprintln!("Error fetching object {:?}: {:?}", path, e);
                        None
                    }
                };
                pb.inc(1);
                result
            }
        })
        .buffer_unordered(16) // Process up to 16 requests concurrently
        .collect::<Vec<_>>()
        .await;

    progress_bar.finish_with_message("Fetching complete");

    let successful_objects: Vec<_> = objects.into_iter().filter_map(|obj| obj).collect();

    println!("Successfully fetched {} objects", successful_objects.len());

    let mut all_results = Vec::new();

    let process_progress_bar = ProgressBar::new(successful_objects.len() as u64);
    process_progress_bar.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("##-"));

    for (path, data) in &successful_objects {
        let size_gb = data.len() as f64 / 1_073_741_824.0; // Convert bytes to GB
        process_progress_bar.set_message(format!("Processing: {:?} (size: {:.2} GB)", path, size_gb));

        match process_parquet(data) {
            Ok(results) => {
                process_progress_bar.set_message(format!("Found {} matching rows in {:?}", results.len(), path));
                all_results.extend(results);
            },
            Err(e) => eprintln!("Error processing {:?}: {:?}", path, e),
        }
        process_progress_bar.inc(1);
    }

    process_progress_bar.finish_with_message("Processing complete");

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

    let schema: SchemaRef = reader.schema().clone();
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
