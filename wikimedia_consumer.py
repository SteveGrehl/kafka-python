#!/usr/bin/env python

import logging
import json
from confluent_kafka import KafkaException
from kafka_tutorial import BasicKafkaConsumer
import numpy as np

class WikimediaChangesConsumer(BasicKafkaConsumer):

    def __init__(self, config: dict, group_id=None) -> None:
        super().__init__(config, group_id)

    def loop_for_messages(self, timeout=10.0) -> None:
        try:
            while True:
                msg = super().poll(timeout=timeout)  # in seconds
                if msg is None:
                    self.logger.info(f"No more messages received after {timeout} seconds")
                    break
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    try:
                        payload = json.loads(json.loads(msg.value()))
                        # Proper message
                        self.logger.debug('%% %s [%d] at offset %d with key %s:\n' %
                                        (msg.topic(), msg.partition(), msg.offset(),
                                        str(msg.key())))
                        if np.all([k in payload for k in ['title', 'comment', 'user']]):
                            print(f"[{payload['title']}] {payload['comment']} (by {payload['user']})")
                    except json.JSONDecodeError as de:
                        self.logger.warning(f"{de} for {msg.value()}")

        except KeyboardInterrupt:
            self.logger.info('%% Aborted by user\n')
        except Exception as ex:
            self.logger.error(f'%% Unexpected Exception {ex}\n')
        finally:
            # Close down consumer to commit final offsets.
            self.logger.info("Shutdown consumer")
            self.close()

def main():
    """default main method
    """
    logging.basicConfig(level=logging.DEBUG, \
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    stream = 'https://stream.wikimedia.org/v2/stream/recentchange'   
    topic = "wikimedia.recentchange"

    logging.info(f"Preparing Kafka-Wikiemedia-Demo for {topic=}")
    # Configuration see: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    params = {
        'bootstrap.servers': 'localhost:9092',
        'ssl.key.location': '/Users/sgrehl/.ssh',
    }
    params_consumer = {
        'group.id': 'my-python-wikimedia-application',
        'auto.offset.reset': 'latest',  # one of: none, earliest, latest
        'partition.assignment.strategy': 'roundrobin',  # one of: range,roundrobin - Coorperative strategies (in contrast to eager) not found in setings
        # 'group.instance.id' static group memebership, stops reassigment for given time
        'auto.commit.interval.ms': 5000, # if timout is reached consumer informs broker to set offset
    }
    consumer: WikimediaChangesConsumer = WikimediaChangesConsumer(
        config={**params, **params_consumer}
        )
    consumer.subscribe(topics=[topic])
    consumer.loop_for_messages(timeout=60.0)

if __name__ == '__main__':
    main()
