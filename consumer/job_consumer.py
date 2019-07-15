import argparse
import sys
from uuid import uuid1
from confluent_kafka import Consumer, KafkaException


def print_assignment(_, partitions):
    print('Assignment:', partitions)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", help="Address for the Kafka broker", default="kafka:29092")
    parser.add_argument("--topic", help="Topic to publish messages to", default="job_queue")
    args = parser.parse_args()

    conf = {'bootstrap.servers': args.broker, 'group.id': 'job_consumer', 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'}

    c = Consumer(conf)
    subscribed = False
    while not subscribed:
        try:
            c.subscribe([args.topic], on_assign=print_assignment)
        except:
            continue
        subscribed = True

    stop_consumer = Consumer({'bootstrap.servers': args.broker, 'group.id': uuid1, 'auto.offset.reset': 'earliest'})

    while True:
        stop_consumer.poll(timeout=1.0)
        msg = c.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Proper message
            sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                             (msg.topic(), msg.partition(), msg.offset(),
                              str(msg.key())))
        print(msg.value())

    c.close()
