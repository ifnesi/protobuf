import argparse

from confluent_kafka import Consumer, KafkaException


########
# Main #
########
def main(args):
    # Configure Kafka consumer
    conf_confluent = {
        "group.id": "test-binary",
        "client.id": "test-binary",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "bootstrap.servers": args.bootstrap_servers,
    }

    consumer = Consumer(conf_confluent)

    try:
        consumer.subscribe(
            [
                args.topic,
            ]
        )

        print(f"\nConsumer started, consuming from topic {args.topic}...\n")
        while True:
            try:
                msg = consumer.poll(timeout=1)
                if msg is None:
                    pass
                elif msg.error():
                    raise KafkaException(msg.error())
                else:
                    print(f"Key: {msg.key()}")
                    print(f"Value: {msg.value()}")

            except Exception as err:
                print(f"\n > ERR: {err}\n")

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume topic as binary, no deserialisation")
    parser.add_argument(
        "-b",
        dest="bootstrap_servers",
        default="localhost:9092",
        help="Bootstrap broker(s) (host[:port])",
    )
    parser.add_argument(
        "-s",
        dest="schema_registry",
        default="http://schema-registry:8081",
        help="Schema Registry (http(s)://host[:port]",
    )
    parser.add_argument(
        "-t",
        dest="topic",
        default="data-sample-protobuf",
        help="Topic name",
    )

    main(parser.parse_args())
