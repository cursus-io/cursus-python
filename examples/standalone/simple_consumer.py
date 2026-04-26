import signal
import sys

from cursus import Consumer, ConsumerConfig, ConsumerMode


def main():
    config = ConsumerConfig(
        brokers=["localhost:9000"],
        topic="hello-topic",
        group_id="hello-group",
        mode=ConsumerMode.STREAMING,
    )

    consumer = Consumer(config)
    signal.signal(signal.SIGINT, lambda *_: (consumer.close(), sys.exit(0)))

    print(f"Waiting for messages on '{config.topic}'. Press Ctrl+C to stop.")
    for msg in consumer:
        print(f"offset={msg.offset:<6}  payload={msg.payload}")


if __name__ == "__main__":
    main()
