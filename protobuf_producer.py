import json
import time
import random
import argparse

from functools import partial


# Protobuf generated class; resides at ./protobuf/user_pb2.py
import user_pb2
from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer


def delivery_report(title, payload, topic, err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print(f"***{title}***")
    print(f"Topic: {topic}")
    print("Payload:")
    print(json.dumps(payload, indent=3))
    print(f"User record successfully produced to topic '{msg.topic()}': partition #{msg.partition()} at offset #{msg.offset()}")
    print("------------------------\n")


def main(args):
    FIRST_NAME = ["James", "John", "Robert", "Michael", "William", "David", "Richard", "Charles", "Joseph", "Thomas", "Andre", "Chris", "Maria", "Samantha", "Anna", "Joanna", "Megan", "Amanda", "Carol", "Julia", "Angelica", "Alexandra", "Becky", "Lisa"]
    LAST_NAME = ["Smith", "Johnson", "Jones", "Williams", "Davis", "Garcia", "Miller", "Martin", "Anderson", "Wilson", "Rodriguez", "Martinez", "Taylor", "Clark", "Thompson", "Walker", "Moore", "Harris", "Campbell", "Hernandez", "Adams", "Robinson", "Scott"]
    COLORS = ["blue", "red", "yellow", "green", "black", "purple", "grey", "white", "orange", "cyan", "magenta"]

    schema_registry_conf = {
        "url": args.schema_registry,
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    protobuf_serializer = ProtobufSerializer(
        user_pb2.User,
        schema_registry_client,
        {
            "use.deprecated.format": False,
        },
    )

    producer_conf = {
        "bootstrap.servers": args.bootstrap_servers,
    }

    producer = Producer(producer_conf)

    print("Producing user records to topics. ^C to exit\n")

    is_confluent_protobuf = True
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        time.sleep(2)
        try:
            user_name = f"{random.choice(FIRST_NAME)} {random.choice(LAST_NAME)}"
            user_favorite_number = random.randrange(1000, 9999)
            user_favorite_color = random.choice(COLORS)
            user = user_pb2.User(
                name=user_name,
                favorite_color=user_favorite_color,
                favorite_number=user_favorite_number,
            )
            payload = {
                "name": user_name,
                "favorite_color": user_favorite_color,
                "favorite_number": user_favorite_number,
            }
            if is_confluent_protobuf:
                # Using Confluent Serialiser
                topic = f"{args.topic}-protobuf"
                delivery_report_cc = partial(
                    delivery_report,
                    "Producing message using Confluent Serialiser (<protobuf>)",
                    payload,
                    topic,
                )
                producer.produce(
                    topic=topic,
                    value=protobuf_serializer(
                        user,
                        SerializationContext(topic, MessageField.VALUE),
                    ),
                    on_delivery=delivery_report_cc,
                )

            else:
                # Using Google's standard protobuf serialiser
                topic = f"{args.topic}-protobuf_nosr"
                delivery_report_google = partial(
                    delivery_report,
                    "Producing message using Google's standard protobuf serialiser (<protobuf_nosr>)",
                    payload,
                    topic,
                )
                producer.produce(
                    topic=topic,
                    value=user.SerializePartialToString(),
                    on_delivery=delivery_report_google,
                )

        except (KeyboardInterrupt, EOFError):
            break

        except ValueError:
            print("Invalid input, discarding record...")

        finally:
            is_confluent_protobuf = not is_confluent_protobuf

    print("\nFlushing...\n")
    producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Protobuf Serializer")
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
