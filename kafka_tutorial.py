#!/usr/bin/env python

import logging
import time
from concurrent.futures import ThreadPoolExecutor
#docs: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
from confluent_kafka import Producer, Message, Consumer, TopicPartition, KafkaException
from confluent_kafka.admin import ClusterMetadata
from confluent_kafka.error import KafkaError


class BasicKafkaProducer(Producer):
    """Custom Kafka Producer class
    adds some callbacks

    Args:
        Producer (confluent_kafka.Producer): base class from python-bindings
    """

    logger = logging.getLogger("BasicKafkaProducerLogger")
    logger.setLevel(logging.INFO)

    def __init__(self, config: dict) -> None:
        self._config = config
        assert "bootstrap.servers" in self._config, \
            f"Missing 'bootstrap.servers' in {self._config=}"
        super().__init__(self._config)

    def produce(self, *, topic:str, message:str, key=None, **kwargs) -> None:
        """overload super method to inject callback function, pass everything else

        Args:
            topic (str): kafka topic
            message (str): value for kafka message

        Returns:
            BasicKafkaProducer: self
        """
        super().produce(
            topic=topic,
            value=message,
            key=key,
            on_delivery=self.on_message_send,
            **kwargs
        )
        return self

    def on_message_send(self, error: KafkaError, msg: Message) -> None:
        """callback after a messsage was send by the producer

        Args:
            error (KafkaError): error, defaults to None
            msg (Message): message after send
        """
        if isinstance(error, KafkaError):
            self.logger.error(error, self._config)
        self.logger.debug(f"[{msg.topic()}-{msg.partition()}-{msg.offset()}] \
            {msg.value()} @ {msg.timestamp()}")


class BasicKafkaConsumer(Consumer):
    """Custom Kafka Consumer class
    implements some custom functions and callbacks

    Args:
        Consumer (confluent_kafka.Consumer): base class from python-bindings
    """
    logger = logging.getLogger("BasicKafkaConsumerLogger")
    logger.setLevel(logging.INFO)

    def __init__(self, config: dict, group_id=None) -> None:
        self._config = config
        if group_id is not None:
            self._config["group.id"] = group_id
        for k in ["bootstrap.servers", "group.id"]:
            assert k in self._config, f"Missing '{k}' in {self._config=}"
        super().__init__(self._config)

    def subscribe(self, topics: list[str]) -> None:
        """topic subscribtion defining all callbacks with classfunctions

        Args:
            topics (list[str]): list of kafka topics
        """
        cls = self.__class__
        super().subscribe(
            topics=topics,
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
            on_lost=self.on_lost,
        )
    
    def loop_for_messages(self, timeout=10.0) -> None:
        """
        run an infinity loop and poll messages, user can stop this with <Ctrl+C>
        """
        try:
            while True:
                msg = super().poll(timeout=timeout)  # in seconds
                if msg is None:
                    self.logger.info(f"No more messages received after {timeout} seconds")
                    break
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    # Proper message
                    self.logger.debug('%% %s [%d] at offset %d with key %s:\n' %
                                    (msg.topic(), msg.partition(), msg.offset(),
                                    str(msg.key())))
                    print(msg.value())

        except KeyboardInterrupt:
            self.logger.info('%% Aborted by user\n')
        except Exception as ex:
            self.logger.error(f'%% Unexpected Exception {ex}\n')
        finally:
            # Close down consumer to commit final offsets.
            self.logger.info("Shutdown consumer")
            self.close()

    def on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """
        callback to provide handling of customized offsets on completion of
        a successful partition re-assignment.
        """
        self.logger.debug(f"Assigned {consumer} to partitions ({[p.partition for p in partitions]})")

    def on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """
        callback to provide handling of offset commits to a customized store on the start of
        a rebalance operation.
        """
        self.logger.info(f"Revoked {consumer} at partitions ({[p.partition for p in partitions]})")

    def on_lost(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """
        callback to provide handling n the case the partition assignment has been lost.
        If not specified, lost partition events will be  delivered to on_revoke, if specified.
        Partitions that  have been lost may already be owned by other members
        in the group and therefore committing offsets, for example, may fail.
        """
        self.logger.warning(f"Lost assigment of {consumer} at partitions ({[p.partition for p in partitions]})")

def main() -> None:
    """default main
    """
    logging.basicConfig(level=logging.DEBUG, \
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    topic = "first-topic"
    logging.info(f"Preparing Kafka-Demo for {topic=}")
    # Configuration see: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    params = {
        'bootstrap.servers': 'localhost:9092',
        'ssl.key.location': '/Users/sgrehl/.ssh',
    }
    params_producer = {
        "acks": 1,
    }
    params_consumer = {
        'group.id': 'my-python-application',
        'auto.offset.reset': 'latest',  # one of: none, earliest, latest
        'partition.assignment.strategy': 'roundrobin',  # one of: range,roundrobin - Coorperative strategies (in contrast to eager) not found in setings
        # 'group.instance.id' static group memebership, stops reassigment for given time
        'auto.commit.interval.ms': 5000, # if timout is reached consumer informs broker to set offset
    }
    producer: BasicKafkaProducer = BasicKafkaProducer({**params, **params_producer})

    n_consumers = 2
    consumers:list[BasicKafkaConsumer] = [None]*n_consumers
    for i in range(n_consumers):
        consumers[i] = BasicKafkaConsumer({**params, **params_consumer})

    cluster_meta: ClusterMetadata = consumers[0].list_topics()
    assert topic in cluster_meta.topics, f"{topic} not in {cluster_meta.topics()}"
    for consumer in consumers:
        consumer.subscribe([topic])

    # Prducer generate messages, TODO: Do in own Thread
    for i in range(100):
        producer.produce(
            topic=topic,
            message=f"Send Message {i}",
            key=None# "my_key"
        )
        time.sleep(0.01)

    producer.flush(60)
    logging.info("Finished producer part")

    # Consumer reads messages, in own Thread
    with ThreadPoolExecutor(max_workers = len(consumers)) as executor:
        for consumer in consumers:
            executor.submit(consumer.loop_for_messages)
    logging.info("Bye")

if __name__ == "__main__":
    main()
