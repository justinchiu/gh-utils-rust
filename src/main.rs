use std::time::Instant;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::list::ListObjectsRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = run().await;

    let bucket_name = "cohere-data";
    let prefix = "dataacq/github-repos/permissive_and_unlicensed/repo-level-rows/";

    let start = Instant::now();

    let mut request = ListObjectsRequest {
        bucket: bucket_name.to_string(),
        prefix: Some(prefix.to_string()),
        ..Default::default()
    };

    let objects = client.list_objects(&request).await?;

    for object in objects.items.into_iter() {
        let object_name = object.name;
        if !object_name.ends_with(".parquet") {
            continue;
        }

        println!("Processing file: {}", object_name);

        let get_request = GetObjectRequest {
            bucket: bucket_name.to_string(),
            object: object_name.clone(),
            ..Default::default()
        };

        let content = client.download_object(&get_request).await?;

        // Process the Parquet file content here
        // For example, you can use the `parquet` crate to read the file
        // let reader = parquet::file::serialized_reader::SerializedFileReader::new(content.as_slice())?;
        // ... process the Parquet data ...
    }

    let end = Instant::now();
    let duration = (end - start).as_secs_f32();
    println!("Duration: {duration} secs");

    Ok(())
}

async fn run() -> Client {
    let config = ClientConfig::default().with_auth().await.unwrap();
    Client::new(config)
}
