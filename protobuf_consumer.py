import json
import argparse

# Protobuf generated class; resides at ./protobuf/user_pb2.py
import user_pb2
from confluent_kafka import Consumer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    SerializationError,
)
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer


def main(args):

    protobuf_deserializer = ProtobufDeserializer(
        user_pb2.User,
        {
            "use.deprecated.format": False,
        },
    )

    consumer_conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": "test-protobuf",
        "client.id": "test-protobuf",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    TOPICS = [
        f"{args.topic}-protobuf",
        f"{args.topic}-protobuf_nosr"
    ]
    consumer = Consumer(consumer_conf)
    consumer.subscribe(TOPICS)

    print(f"Consuming user records from topics ', '.join({TOPICS}). ^C to exit.")
    while True:
        try:
            msg = consumer.poll(0.15)
            if msg is not None:
                try:
                    # Trying to deserialise using Confluent Deseriliser
                    user = protobuf_deserializer(
                        msg.value(),
                        SerializationContext(args.topic, MessageField.VALUE),
                    )
                    print("Deserializer: <protobuf>")

                except SerializationError:
                    # Unknown magic byte Exception
                    # # Trying to deserialise using Google's standard protobuf serialiser
                    user = user_pb2.User()
                    user.ParseFromString(msg.value())
                    print("Deserializer: <protobuf_nosr>")

                except Exception as err:
                    user = None
                    print(f"ERROR: {err}")

                if user is not None:
                    print(f"Binary message value: {msg.value()}")
                    print(
                        json.dumps(
                            {
                                "name": user.name,
                                "favorite_color": user.favorite_color,
                                "favorite_number": user.favorite_number,
                            },
                            indent=3,
                        )
                    )
                    print("------------------------\n")

        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Protobuf Deserializer")
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
        default="data-sample",
        help="Topic name",
    )

    main(parser.parse_args())
