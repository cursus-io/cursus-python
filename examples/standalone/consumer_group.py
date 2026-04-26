import signal
import sys
import threading

from cursus import Consumer, ConsumerConfig, ConsumerMode


def run_consumer(name: str, config: ConsumerConfig):
    consumer = Consumer(config)
    print(f"[{name}] Started")

    def on_signal(*_):
        consumer.close()

    signal.signal(signal.SIGINT, on_signal)

    for msg in consumer:
        print(f"[{name}] offset={msg.offset} payload={msg.payload}")


def main():
    threads = []
    for i in range(3):
        config = ConsumerConfig(
            brokers=["localhost:9000"],
            topic="orders",
            group_id="order-processors",
            mode=ConsumerMode.STREAMING,
        )
        t = threading.Thread(target=run_consumer, args=(f"consumer-{i}", config), daemon=True)
        t.start()
        threads.append(t)

    print("3 consumers started in group 'order-processors'. Press Ctrl+C to stop.")
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
