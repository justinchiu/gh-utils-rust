use std::time::Instant;
use google_cloud_storage::client::{ClientConfig, Client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = run().await;

    let bucket_name = "cohere-data";
    let prefix = "dataacq/github-repos/permissive_and_unlicensed/repo-level-rows/";

    let start = Instant::now();

    let mut objects = client.object().list(bucket_name, prefix).await?;

    while let Some(object) = objects.next().await {
        let object = object?;
        let object_name = object.name;
        if !object_name.ends_with(".parquet") {
            continue;
        }

        println!("Processing file: {}", object_name);

        let content = client
            .object()
            .download(bucket_name, &object_name)
            .await?;

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
    let client = Client::new(config);
    client
}
