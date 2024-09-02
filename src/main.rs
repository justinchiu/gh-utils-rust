use std::time::Instant;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::list::ListObjectsRequest;
use std::default::Default;
use futures::stream::{self, StreamExt};

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

    println!("Number of parquet files: {}", object_requests.len());

    let objects = stream::iter(object_requests)
        .map(|request| {
            let client = client.clone();
            async move {
                match client.get_object(&request).await {
                    Ok(object) => Some(object),
                    Err(e) => {
                        eprintln!("Error fetching object {}: {:?}", request.object, e);
                        None
                    }
                }
            }
        })
        .buffer_unordered(10) // Process up to 10 requests concurrently
        .collect::<Vec<_>>()
        .await;

    let successful_objects: Vec<_> = objects.into_iter().filter_map(|obj| obj).collect();

    println!("Successfully fetched {} objects", successful_objects.len());

    for object in &successful_objects {
        let size_gb = object.size as f64 / 1_073_741_824.0; // Convert bytes to GB
        println!("Object: {} (size: {:.2} GB)", object.name, size_gb);
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
