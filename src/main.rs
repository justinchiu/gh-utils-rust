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

    let mut all_results = Vec::new();

    let objects = stream::iter(objects)
        .map(|path| {
            let store = Arc::clone(&store);
            let pb = progress_bar.clone();
            async move {
                match store.get(&path).await {
                    Ok(object) => {
                        let data = object.bytes().await.ok()?;
                        let size_gb = data.len() as f64 / 1_073_741_824.0; // Convert bytes to GB
                        pb.set_message(format!("Processing: {:?} (size: {:.2} GB)", path, size_gb));
                        
                        match process_parquet(&data) {
                            Ok(results) => {
                                pb.set_message(format!("Found {} matching rows in {:?}", results.len(), path));
                                Some(results)
                            },
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
            }
        })
        .buffer_unordered(16) // Process up to 16 requests concurrently
        .filter_map(|result| async move { result })
        .for_each(|results| {
            all_results.extend(results);
            progress_bar.inc(1);
            futures::future::ready(())
        })
        .await;

    progress_bar.finish_with_message("Fetching and processing complete");

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
