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

    let mut request = ListObjectsRequest {
        bucket: bucket_name.to_string(),
        prefix: Some(prefix.to_string()),
        ..Default::default()
    };

    let objects = client.list_objects(&request).await?;

    if let Some(items) = objects.items {
        for object in items {
            if let Some(object_name) = object.name.clone() {
                if !object_name.ends_with(".parquet") {
                    continue;
                }

                println!("Processing file: {}", object_name);

                let get_request = GetObjectRequest {
                    bucket: bucket_name.to_string(),
                    object: object_name.clone(),
                    ..Default::default()
                };

                let content = client.download_object(&get_request, &Default::default()).await?;

                // Process the Parquet file content here
                // For example, you can use the `parquet` crate to read the file
                // let reader = parquet::file::serialized_reader::SerializedFileReader::new(content.as_slice())?;
                // ... process the Parquet data ...
            }
        }
    } else {
        println!("No objects found in the bucket with the given prefix.");
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
