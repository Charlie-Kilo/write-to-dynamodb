use rusoto_dynamodb::{AttributeValue, DynamoDb, DynamoDbClient, PutItemInput};
use rusoto_core::Region;
use rusoto_s3::{ListObjectsV2Request, S3, S3Client};
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use warp::{Filter, Rejection, Reply};

#[derive(Debug, Deserialize)]
struct ImageMetadataWithKey {
    url: String,
    label: String,
    type_: String,
    season: String,
    show_name: String,
    designer: String,
    description: String,
    request_id: String,
    final_image_key: Option<String>, // Make final_image_key an Option<String>
}

#[derive(Debug, Deserialize)]
struct ImageMetadataNoKey {
    url: String,
    label: String,
    type_: String,
    season: String,
    show_name: String,
    designer: String,
    description: String,
    request_id: String,
}

// Function to query S3 bucket and get the final image path
async fn get_final_image_key(request_id: &str) -> Result<Option<String>, Box<dyn Error>> {
    // Create an S3 client
    let s3_client = S3Client::new(Region::default());

    // Construct the key prefix to fetch from S3
    let key_prefix = format!("images/{}/", request_id);
    println!("Key prefix: {}", key_prefix);

    // Construct the request to list objects in the S3 bucket
    let request = ListObjectsV2Request {
        bucket: "team-3-project-3".to_string(),
        prefix: Some(key_prefix),
        ..Default::default()
    };

    // Send the request to S3
    let response = s3_client.list_objects_v2(request).await?;

    // Extract the contents from the response
    let contents = response.contents.unwrap_or_else(Vec::new);

    // Extract the last object key if available
    if let Some(object) = contents.last() {
        let object_key = object.key.as_ref().ok_or("No object key in S3 response")?;
        // If the object key is found, construct the final image path
        let final_image_key = format!("s3://{}/{}", "team-3-project-3", object_key);
        println!("Final image key: {}", final_image_key);
        Ok(Some(final_image_key))
    } else {
        // If no objects are found, return None
        println!("No objects found in S3");
        Ok(None)
    }
}

impl ImageMetadataWithKey {
    fn to_attribute_values(&self) -> Vec<(String, AttributeValue)> {
        let mut item_values = vec![
            ("url".to_owned(), AttributeValue { s: Some(self.url.clone()), ..Default::default() }),
            ("label".to_owned(), AttributeValue { s: Some(self.label.clone()), ..Default::default() }),
            ("type".to_owned(), AttributeValue { s: Some(self.type_.clone()), ..Default::default() }),
            ("season".to_owned(), AttributeValue { s: Some(self.season.clone()), ..Default::default() }),
            ("show_name".to_owned(), AttributeValue { s: Some(self.show_name.clone()), ..Default::default() }),
            ("designer".to_owned(), AttributeValue { s: Some(self.designer.clone()), ..Default::default() }),
            ("description".to_owned(), AttributeValue { s: Some(self.description.clone()), ..Default::default() }),
            ("request_id".to_owned(), AttributeValue { s: Some(self.request_id.clone()), ..Default::default() }),
        ];

        // Include the final image path if it's present
        if let Some(final_image_key) = &self.final_image_key {
            item_values.push((
                "final_image_key".to_owned(),
                AttributeValue { s: Some(final_image_key.clone()), ..Default::default() },
            ));
        }

        item_values
    }
}

async fn write_to_dynamodb(
    client: &DynamoDbClient,
    table_name: String,
    metadata: ImageMetadataWithKey,
) -> Result<(), Box<dyn Error>> {
    println!("Received metadata: {:?}", metadata);
    let item = metadata.to_attribute_values();
    let mut item_map = HashMap::new();
    for (k, v) in item {
        item_map.insert(k, v);
    }

    let input = PutItemInput {
        item: item_map,
        table_name,
        ..Default::default()
    };

    client.put_item(input).await?;
    println!("Successfully wrote metadata to DynamoDB");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = DynamoDbClient::new(Region::default());

    let api_route = warp::post()
        .and(warp::path("upload"))
        .and(warp::body::json())
        .and(warp::any().map(move || client.clone()))
        .and_then(handle_request)
        .with(warp::log("api"));

    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["POST"])
        .allow_headers(vec![
            "Content-Type",
            "Authorization",
        ])
        .build();

    let route_with_cors = api_route.with(cors);

    warp::serve(route_with_cors)
        .run(([127, 0, 0, 1], 3033))
        .await;

    Ok(())
}

async fn get_image_metadata_with_key(
    metadata: ImageMetadataNoKey,
) -> Result<ImageMetadataWithKey, Box<dyn Error>> {
    let final_image_key = match get_final_image_key(&metadata.request_id).await {
        Ok(Some(key)) => key,
        _ => String::new(),
    };

    Ok(ImageMetadataWithKey {
        url: metadata.url,
        label: metadata.label,
        type_: metadata.type_,
        season: metadata.season,
        show_name: metadata.show_name,
        designer: metadata.designer,
        description: metadata.description,
        request_id: metadata.request_id,
        final_image_key: Some(final_image_key), // Ensure final_image_key is wrapped in Some
    })
}

async fn handle_request(
    metadata: ImageMetadataNoKey,
    client: DynamoDbClient,
) -> Result<impl Reply, Rejection> {
    let metadata_with_key = match get_image_metadata_with_key(metadata).await {
        Ok(metadata) => metadata,
        Err(err) => {
            eprintln!("Error getting image metadata with key: {}", err);
            return Err(warp::reject::reject());
        }
    };

    match write_to_dynamodb(&client, "project-3-testing".to_string(), metadata_with_key).await {
        Ok(_) => Ok(warp::reply::html("Successfully received metadata and wrote to DynamoDB")),
        Err(err) => {
            eprintln!("Error: {}", err);
            Err(warp::reject::reject())
        }
    }
}
