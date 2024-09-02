use std::time::Instant;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::list::ListObjectsRequest;
use std::default::Default;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = run().await?;

    let bucket_name = "cohere-data";
    let prefix = "dataacq/github-repos/permissive_and_unlicensed/repo-level-rows/";

    let start = Instant::now();

    let request = ListObjectsRequest {
        bucket: bucket_name.to_string(),
        prefix: Some(prefix.to_string()),
        ..Default::default()
    };

    let objects = client.list_objects(&request).await?;
    let items = match objects.items {
        Some(xs) => xs,
        None => panic!("Unable to fulfill GCS request"),
    };

    let object_requests: Vec<_> = items
        .into_iter()
        .filter(|item| item.name.ends_with(".parquet"))
        .map(|item| {
            GetObjectRequest {
                bucket: bucket_name.to_string(),
                object: item.name,
                ..Default::default()
            }
        })
        .collect();

    println!("Number of parquet files: {}", parquet_files.len());

    for request in &parquet_files {
        println!("Object: {}", request.object);
    }

    let end = Instant::now();
    let duration = (end - start).as_secs_f32();
    println!("Duration: {duration} secs");

    Ok(())
}

async fn run() -> Result<Client, Box<dyn std::error::Error>> {
    let config = ClientConfig::default().with_auth().await?;
    Ok(Client::new(config))
}
