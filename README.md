# protobuf
Producer/Consumer using protobuf and protobuf_nosr

Python scripts to produce/consume events using both Google's Protobuf serialisation and [Confluent's](https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/serdes-protobuf.html) (appending five bytes to the serialised event)

By default, messages will be produced to the topics:
 - `data-sample-protobuf`: Confluent Protobuf serialisation
 - `data-sample-protobuf_nosr`: Google Protobuf serialisation

The consumer will first try to deserialise using Confluent Protobuf serialisation, but in case of exception `SerializationError` ([Unknown magic byte](https://www.confluent.io/en-gb/blog/how-to-fix-unknown-magic-byte-errors-in-apache-kafka)) will try Google's

Protobuf schema (see file `user.proto`):
```
syntax = "proto3";

message User {
    string name = 1;
    int64 favorite_number = 2;
    string favorite_color = 3;
}
```

## Requirements
- [Docker Desktop + Compose](https://www.docker.com/products/docker-desktop)
- [librdkafka](https://github.com/confluentinc/librdkafka) (`brew install librdkafka`)
- [Python 3.8+](https://www.python.org/downloads/)
  - Install requirements (`python3 -m pip install -r requirements.txt`)

## :white_check_mark: Start the demo
 - Run docker desktop
 - Start docker compose (Confluent Platform): `docker-compose up -d`
 - Wait couple of minutes until you are able to access C3: http://localhost:9021
 - On one terminal window start the producer: `python3 protobuf_producer.py`
 - On another terminal window start the consumer: `python3 protobuf_consumer.py`

### Output examples

#### Producer (python3 protobuf_producer.py)
```
***Producing message using Confluent Serialiser (<protobuf>)***
Topic: data-sample-protobuf
Payload:
{
   "name": "Amanda Johnson",
   "favorite_color": "yellow",
   "favorite_number": 9222
}
User record successfully produced to topic 'data-sample-protobuf': partition #0 at offset #36
------------------------

***Producing message using Google's standard protobuf serialiser (<protobuf_nosr>)***
Topic: data-sample-protobuf_nosr
Payload:
{
   "name": "Thomas Rodriguez",
   "favorite_color": "cyan",
   "favorite_number": 4493
}
User record successfully produced to topic 'data-sample-protobuf_nosr': partition #0 at offset #34
```

#### Consumer (python3 protobuf_consumer.py)
```
Deserializer: <protobuf_nosr>
Binary message value: b'\n\x0eAmanda Johnson\x10\x86H\x1a\x06yellow'
{
   "name": "Amanda Johnson",
   "favorite_color": "yellow",
   "favorite_number": 9222
}
------------------------

Deserializer: <protobuf>
Binary message value: b'\x00\x00\x00\x00\x01\x00\n\x10Thomas Rodriguez\x10\x8d#\x1a\x04cyan'
{
   "name": "Thomas Rodriguez",
   "favorite_color": "cyan",
   "favorite_number": 4493
}
------------------------
```

### ksqlDB
Protobuf serialised topics can also be ingested through ksqlDB (all credits here to the great [Luiz Roma](https://github.com/romajr)).
1. First you need to access Confluent Control Center: http://localhost:9021/clusters/
2. Select the cluster `controlcenter.cluster` (the only one on that docker compose file)
3. On the left hand side menu select ksqlDB, the select the cluster `ksqldb1` (the only cluster on that docker compose file)
4. The first tab will be the ksqlDB editor where you can ingest topic data, query them and create your streaming applications
5. Make sure the `auto.offset.reset` option is set to `Earliest`

#### Serialisers
1. Confluent Protobuf serialisation:
   - Enter the following query `CREATE STREAM deserialized_protobuf WITH (kafka_topic='data-sample-protobuf', value_format='protobuf');`
   - Click `Run Query`
   - If all goes well, you should see the confirmation message as shown below:
```
{
  "@type": "currentStatus",
  "statementText": "CREATE STREAM DESERIALIZED_PROTOBUF (NAME STRING, FAVORITE_NUMBER BIGINT, FAVORITE_COLOR STRING) WITH (CLEANUP_POLICY='delete', KAFKA_TOPIC='data-sample-protobuf', KEY_FORMAT='KAFKA', VALUE_FORMAT='PROTOBUF');",
  "commandId": "stream/`DESERIALIZED_PROTOBUF`/create",
  "commandStatus": {
    "status": "SUCCESS",
    "message": "Stream created",
    "queryId": null
  },
  "commandSequenceNumber": 10,
  "warnings": [

  ]
}
```
2. Google Protobuf serialisation: Note it will be required to use the value format `protobuf_nosr` as the data is serialised using the standard Google protobuf serialisation
   - Enter the following query `CREATE STREAM deserialized_protobuf_nosr (name string, favorite_number int, favorite_color string) WITH (kafka_topic='data-sample-protobuf_nosr', value_format='protobuf_nosr');`
   - Click `Run Query`
   - If all goes well, you should see the confirmation message as shown below:
```
{
  "@type": "currentStatus",
  "statementText": "CREATE STREAM DESERIALIZED_PROTOBUF_NOSR (NAME STRING, FAVORITE_NUMBER INTEGER, FAVORITE_COLOR STRING) WITH (CLEANUP_POLICY='delete', KAFKA_TOPIC='data-sample-protobuf_nosr', KEY_FORMAT='KAFKA', VALUE_FORMAT='PROTOBUF_NOSR');",
  "commandId": "stream/`DESERIALIZED_PROTOBUF_NOSR`/create",
  "commandStatus": {
    "status": "SUCCESS",
    "message": "Stream created",
    "queryId": null
  },
  "commandSequenceNumber": 12,
  "warnings": [

  ]
}
```

To query the newly created streams, try:

1. Confluent Protobuf serialisation: `select * from DESERIALIZED_PROTOBUF EMIT CHANGES;`
![Confluent Protobuf serialisation](/docs/protobuf.png "Confluent Protobuf serialisation")

2. Google Protobuf serialisation: `select * from DESERIALIZED_PROTOBUF_NOSR EMIT CHANGES;`
![Google Protobuf serialisation](/docs/protobuf_nosr.png "Google Protobuf serialisation")

## :x: Stop the demo
 - Press CTRL-C on the producer/consumer terminals
 - Stop docker compose: ```docker-compose down```