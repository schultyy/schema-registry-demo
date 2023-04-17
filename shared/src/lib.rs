use serde::{Deserialize, Serialize, Serializer};
pub const OPEN_WEB_DATA_NAMESPACE: &str = "com.cisco.eti.chatmessage";
pub const CHAT_MSG_NAME: &str = "ChatMessageSent";

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct ChatMessage {
    pub email: String,
    pub message: String
}

#[derive(Debug)]
pub enum AvroData {
    ChatMessageSent(ChatMessage)
}

impl SchemaName for AvroData {
    fn get_full_schema_name(&self) -> String {
        match self {
            AvroData::ChatMessageSent(v) => v.get_full_schema_name(),
        }
    }

    fn get_schema_name(&self) -> &'static str {
        match self {
            AvroData::ChatMessageSent(v) => v.get_schema_name(),
        }
    }
}

pub trait SchemaName {
    fn get_full_schema_name(&self) -> String;
    fn get_schema_name(&self) -> &'static str;
}


impl Serialize for AvroData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            AvroData::ChatMessageSent(v) => v.serialize(serializer),
        }
    }
}

impl SchemaName for ChatMessage {
    fn get_full_schema_name(&self) -> String {
        format!("{}.{}", OPEN_WEB_DATA_NAMESPACE, CHAT_MSG_NAME)
    }
    fn get_schema_name(&self) -> &'static str {
        CHAT_MSG_NAME
    }
}
