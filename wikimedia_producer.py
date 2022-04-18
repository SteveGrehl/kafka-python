#!/usr/bin/env python

import logging
import json
import asyncio
import aiohttp
from aiosseclient import aiosseclient
from kafka_tutorial import BasicKafkaProducer  #pylint-disable: import-error
import numpy as np

class WikimediaChangesProducer(BasicKafkaProducer):
    """catch messages from a wikimedia-stream by using aiosseclient

    Args:
        BasicKafkaProducer (kafka_tutorial.BasicKafkaProducer): kafka producer with some logging
    """

    def __init__(self, stream_url: str, topic: str, config: dict) -> None:
        """default constructor

        Args:
            stream_url (str): stream url from wikimedia
            topic (str): kafka topic to produce for
            config (dict): configuration parameters passed to confluent_kafka.Producer
        """
        self._stream_url = stream_url
        self._topic = topic
        super().__init__(config)

    async def on_new_change(self, key=None) -> None:
        """
        asyncronous reading from wikimedia-steam and producing into kafka-topic
        """
        async for event in aiosseclient(self._stream_url):
            if event.event == "message":
                try:
                    change = json.loads(event.data)
                    rules = [
                        'minor' in change and not change['minor'],
                        # 'server_name' in change and 'de' in change['server_name'],
                        'bot' in change and not change['bot'],
                    ]
                    if np.all(rules):
                        self.logger.debug(event.data)
                        super().produce(
                            topic=self._topic,
                            message=json.dumps(event.data).encode('utf-8'),
                            key=change['server_name'],
                            )
                except ValueError:
                    pass

def main():
    """default main method
    """
    logging.basicConfig(level=logging.DEBUG, \
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    stream = 'https://stream.wikimedia.org/v2/stream/recentchange'   
    topic = "wikimedia.recentchange"

    logging.info(f"Preparing Kafka-Wikiemedia-Demo Producer for {stream=} on {topic=}")
    # Configuration see: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    params = {
        'bootstrap.servers': 'localhost:9092',
        'ssl.key.location': '/Users/sgrehl/.ssh',
    }
    params_producer = {
        "acks": 'all',  # one of: 0, 1, 'all', -1
        "enable.idempotence": True, # make sure every message is only received once by the brokers
        "delivery.timeout.ms": 30000, # fail after 30s
        "max.in.flight.requests.per.connection": 5,

        "compression.type": 'none',  # one of: none, gzip, snappy, lz4, zstd
        "linger.ms": 100,  # wait ms then send
        "batch.size": 64*1024,  #in Bytes

    }
    producer: WikimediaChangesProducer = WikimediaChangesProducer(
        stream_url=stream,
        topic=topic,
        config={**params, **params_producer}
        )
    loop = asyncio.get_event_loop()
    while(True):
        try:
            loop.run_until_complete(producer.on_new_change())
        except aiohttp.ClientPayloadError:
            continue

if __name__ == '__main__':
    main()
