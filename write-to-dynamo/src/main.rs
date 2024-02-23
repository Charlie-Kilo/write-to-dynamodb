use std::error::Error;
use rusoto_core::Region;
use rusoto_dynamodb::{AttributeValue, DynamoDb, DynamoDbClient, PutItemInput};

// Define a struct to represent your data
#[derive(Debug)]
struct ImageMetadata {
    season: String,
    show_name: String,
    designer: String,
    description: String,
    final_image_key: String, // Updated field name
    label: String,
    type_: String,
}

// Implement a method to convert your struct to DynamoDB `AttributeValue`
impl ImageMetadata {
    fn to_attribute_values(&self) -> Vec<(String, AttributeValue)> {
        vec![
            ("Season".to_owned(), AttributeValue { s: Some(self.season.clone()), ..Default::default() }),
            ("ShowName".to_owned(), AttributeValue { s: Some(self.show_name.clone()), ..Default::default() }),
            ("Designer".to_owned(), AttributeValue { s: Some(self.designer.clone()), ..Default::default() }),
            ("Description".to_owned(), AttributeValue { s: Some(self.description.clone()), ..Default::default() }),
            ("final_image_key".to_owned(), AttributeValue { s: Some(self.final_image_key.clone()), ..Default::default() }),
            ("Label".to_owned(), AttributeValue { s: Some(self.label.clone()), ..Default::default() }),
            ("Type".to_owned(), AttributeValue { s: Some(self.type_.clone()), ..Default::default() }),
        ]
    }
}

// Function to write to DynamoDB
async fn write_to_dynamodb(client: DynamoDbClient, table_name: String, metadata: ImageMetadata) -> Result<(), Box<dyn Error>> {
    let item = metadata.to_attribute_values();
    let mut item_map = std::collections::HashMap::new();
    for (k, v) in item {
        item_map.insert(k, v);
    }

    let input = PutItemInput {
        item: item_map,
        table_name: table_name,
        ..Default::default()
    };

    client.put_item(input).await?;
    println!("Successfully wrote metadata to DynamoDB");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create a client
    let client = DynamoDbClient::new(Region::default());

    // Example metadata
    let metadata = ImageMetadata {
        season: "FW/04".to_string(),
        show_name: "The High Streets".to_string(),
        designer: "Takahiro Miyashita".to_string(),
        description: "This is an example description".to_string(),
        final_image_key: "final_images/1/final_image1.jpg".to_string(), // Updated field name
        label: "Number Nine".to_string(),
        type_: "pant".to_string(),
    };

    // Table name in DynamoDB
    let table_name = "project-3-testing".to_string();

    // Write metadata to DynamoDB
    write_to_dynamodb(client, table_name, metadata).await?;

    Ok(())
}
