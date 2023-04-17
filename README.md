# Schema Registry with Rust


## Start Kafka

```bash
$ docker-compose up
```

## Create a Topic

```bash
$ docker-compose exec broker kafka-topics --create --bootstrap-server \
localhost:9092 --replication-factor 1 --partitions 1 --topic chatroom-1
```

## Create a new schema

```bash
jq '. | {schema: tojson}' schema.avsc | curl -XPOST http://localhost:8085/subjects/chat_message/versions -H 'Content-Type: application/json' -d @-
```

## Run the code

```bash
$ cargo run -p consumer
```

In a separate terminal window, run:

```bash
$ cargo run -p producer
```
