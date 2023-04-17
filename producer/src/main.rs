use std::time::Duration;

use fake::{faker::{internet::en::FreeEmail, lorem::en::Paragraph}, Fake};
use rdkafka::{ClientConfig, producer::{FutureProducer, FutureRecord}};
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::async_impl::avro::AvroEncoder;
use shared::{AvroData, ChatMessage};

fn schema_registry_address() -> &'static str {
    "localhost:8085"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let email : String = FreeEmail().fake();
    let text : String = Paragraph(3..5).fake();

    let payload = AvroData::ChatMessageSent(ChatMessage {email, message: text });
    produce(payload, "localhost:29092", "chatroom-1").await?;
    Ok(())
}

async fn produce(payload : AvroData, brokers: &str, topic_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let sr_settings = SrSettings::new(format!("http://{}", schema_registry_address()));
    let encoder = AvroEncoder::new(sr_settings);
    let existing_schema_strategy = SubjectNameStrategy::RecordNameStrategy(String::from("chat_message"));
    let bytes = encoder.encode_struct(&payload, &existing_schema_strategy).await?;

    let delivery_status = producer
        .send(
            FutureRecord::to(&topic_name.to_string())
                .key(&format!("Key {}", -1))
                .payload(&bytes),
            Duration::from_secs(0)
        )
    .await;

    println!("Delivery status for message {:?} received", delivery_status);

    Ok(())
}
