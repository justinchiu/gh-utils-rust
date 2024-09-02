use std::time::Instant;
use std::sync::Arc;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ObjectStore, path::Path};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use bytes::Bytes;
use arrow::array::StringArray;
use arrow::record_batch::RecordBatchReader;
use indicatif::{ProgressBar, ProgressStyle};
use futures::StreamExt;

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
        match store.get(path).await {
            Ok(object) => {
                let data = object.bytes().await?;
                let size_gb = data.len() as f64 / 1_073_741_824.0; // Convert bytes to GB
                match process_parquet(&data) {
                    Ok(mut results) => all_results.append(&mut results),
                    Err(e) => eprintln!("Error processing {:?}: {:?}", path, e),
                }
            },
            Err(e) => eprintln!("Error fetching object {:?}: {:?}", path, e),
        }
    }
    progress_bar.finish();

    println!("Total processed repos: {}", all_results.len());

    let end = Instant::now();
    let duration = (end - start).as_secs_f32();
    println!("Duration: {duration} secs");

    Ok(())
}

fn process_parquet(data: &[u8]) -> Result<Vec<Info>, Box<dyn std::error::Error>> {
    let bytes = Bytes::from(data.to_vec());
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let reader = builder.build()?;

    let schema = reader.schema();
    let mut results = Vec::new();

    for batch in reader {
        let batch = batch?;
        let repo_column = batch.column(schema.index_of("repo")?).as_any().downcast_ref::<StringArray>().unwrap();
        let num_lines_column = batch.column(schema.index_of("num_lines")?).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        let num_files_column = batch.column(schema.index_of("num_files")?).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        let has_tests_column = batch.column(schema.index_of("has_tests")?).as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
        let has_docs_column = batch.column(schema.index_of("has_docs")?).as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();

        for i in 0..batch.num_rows() {
            let info = Info {
                repo: repo_column.value(i).to_string(),
                num_lines: num_lines_column.value(i) as usize,
                num_files: num_files_column.value(i) as usize,
                has_tests: has_tests_column.value(i),
                has_docs: has_docs_column.value(i),
            };
            results.push(info);
        }
    }

    Ok(results)
}
