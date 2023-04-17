use apache_avro::types::Value;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use schema_registry_converter::async_impl::avro::AvroDecoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::avro_common::DecodeResultWithSchema;
use shared::AvroData;

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        println!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("Committing offsets: {:?}", result);
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;

async fn consume_and_print(brokers: &str, group_id: &str, topics: &[&str]) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");
    let sr_settings = SrSettings::new(format!("http://{}", "localhost:8085"));
    let decoder = AvroDecoder::new(sr_settings);

    loop {
        match consumer.recv().await {
            Err(e) => eprintln!("Kafka error: {}", e),
            Ok(m) => {
                match m.key_view::<str>() {
                    Some(Ok(_key)) => match decoder.decode_with_schema(m.payload()).await {
                        Ok(tuple) => match get_avro_data(tuple) {
                            Some(v) => {
                                println!("RECEIVED V: {:?}", v);
                            },
                            None => eprintln!("Could not get avro data"),
                        }
                        Err(e) => eprintln!("Error decoding value of record with error {:?}", e),
                    },
                        Some(Err(_)) => eprintln!("Message payload is not a string"),
                    None => eprintln!("No key")
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

fn get_avro_data(decode_result: Option<DecodeResultWithSchema>) -> Option<AvroData> {
    decode_result
    .and_then(|decode_result| { Some((decode_result.name.unwrap().namespace, decode_result.value)) })
    .and_then(|(namespace, value)| {
            match namespace {
                CHAT_MSG_NAME if CHAT_MSG_NAME == namespace => {
                    println!("{:?}", value);
                    extract_data(value)
                },
                other =>  {
                        eprintln!("unknown data type: {:?}", other);
                        None
                }
            }
        })
}

fn extract_string(value: &Value) -> String {
    if let Value::String(x) = value {
        return x.to_string()
    }
    else {
        panic!("Expected to extract string")
    }
}

fn extract_data(value: Value) -> Option<AvroData> {
    match value {
        Value::Record(values) => {
            let msg = shared::ChatMessage {
                email: extract_string(&values[0].1),
                message: extract_string(&values[1].1)
            };
            Some(AvroData::ChatMessageSent(msg))
        }
        _ => None
    }
}

#[tokio::main]
async fn main() {
    let topics = vec!["chatroom-1"];
    let brokers = "localhost:29092";
    let group_id = "1";

    consume_and_print(brokers, group_id, &topics).await
}
