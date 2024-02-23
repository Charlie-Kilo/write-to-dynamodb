use std::error::Error;
use rusoto_core::Region;
use rusoto_dynamodb::{AttributeValue, DynamoDb, DynamoDbClient, PutItemInput};
use serde::Deserialize;
use warp::{Filter, Rejection, Reply};

// Define a struct to represent your data
#[derive(Debug, Deserialize)]
struct ImageMetadata {
    season: String,
    show_name: String,
    designer: String,
    description: String,
    final_image_key: String,
    label: String,
    type_: String,
}

// Implement a method to convert your struct to DynamoDB `AttributeValue`
impl ImageMetadata {
    fn to_attribute_values(&self) -> Vec<(String, AttributeValue)> {
        vec![
            ("season".to_owned(), AttributeValue { s: Some(self.season.clone()), ..Default::default() }),
            ("show_name".to_owned(), AttributeValue { s: Some(self.show_name.clone()), ..Default::default() }),
            ("designer".to_owned(), AttributeValue { s: Some(self.designer.clone()), ..Default::default() }),
            ("description".to_owned(), AttributeValue { s: Some(self.description.clone()), ..Default::default() }),
            ("final_image_key".to_owned(), AttributeValue { s: Some(self.final_image_key.clone()), ..Default::default() }),
            ("label".to_owned(), AttributeValue { s: Some(self.label.clone()), ..Default::default() }),
            ("type".to_owned(), AttributeValue { s: Some(self.type_.clone()), ..Default::default() }),
        ]
    }
}

// Function to write to DynamoDB
async fn write_to_dynamodb(client: &DynamoDbClient, table_name: String, metadata: ImageMetadata) -> Result<(), Box<dyn Error>> {
    let item = metadata.to_attribute_values();
    let mut item_map = std::collections::HashMap::new();
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

// Define the API endpoint route
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create a client
    let client = DynamoDbClient::new(Region::default());

    // Define the API endpoint route
    let api_route = warp::post()
        .and(warp::path("upload"))
        .and(warp::body::json())
        .and(warp::any().map(move || client.clone()))
        .and_then(handle_request);

    // Start the warp server
    warp::serve(api_route)
        .run(([127, 0, 0, 1], 3030))
        .await;

    Ok(())
}

async fn handle_request(metadata: ImageMetadata, client: DynamoDbClient) -> Result<impl Reply, Rejection> {
    // Example metadata
    let table_name = "project-3-testing".to_string();

    // Write metadata to DynamoDB
    match write_to_dynamodb(&client, table_name, metadata).await {
        Ok(_) => {
            // Respond with success message
            Ok(warp::reply::html("Successfully received metadata and wrote to DynamoDB"))
        }
        Err(err) => {
            // Log the error
            eprintln!("Error: {}", err);
            // Respond with error message
            Err(warp::reject::reject())
        }
    }
}
