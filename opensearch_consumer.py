#!/usr/bin/env python

from __future__ import annotations
import logging
from typing import Dict
import json
import yaml
from elasticsearch import Elasticsearch, helpers
from kafka_tutorial import BasicKafkaConsumer
import uuid

class OpenSearchConsumer(BasicKafkaConsumer):

    logger = logging.getLogger("OpenSearchConsumerLogger")
    logger.setLevel(logging.INFO)

    def __init__(self, config: dict, es_header: list[Dict], group_id=None) -> OpenSearchConsumer:
        """default constructor

        Args:
            config (dict): kafka consumer properties
            es_header (list[dict]): opensearch header information
            group_id (str, optional): group.id for kafka (overwrites properties). Defaults to None.

        Returns:
            OpenSearchConsumer: self
        """        
        self._es_header = es_header
        self._es_client = None
        super().__init__(config, group_id)

    def opensearch_connect(self) -> OpenSearchConsumer:
        """connect to opensearch database

        Returns:
            OpenSearchConsumer: self
        """        
        if self._es_client is None:
            self.logger.info(f"Connecting to {self._es_header[0]['host']}:{self._es_header[0]['port']} with key {self._es_header[0]['http_auth'][0]}")
            self._es_client: Elasticsearch = Elasticsearch(self._es_header)
            self._es_client.ping()
        return self
    
    def create_index(self, index_name:str) -> OpenSearchConsumer:
        """ create index in opensearch database
        see: https://towardsdatascience.com/creating-and-managing-elasticsearch-indices-with-python-f676ff1c8113
        Args:
            index_name (str): name of the index

        Returns:
            OpenSearchConsumer: self
        """        
        if self._es_client is None:
            self.logger.warning(f"_es_client is None - No index '{index_name}' \
                 created - Please run opensearch_connect()")
            return self
        if self._es_client.indices.exists(index=index_name):
            self.logger.info(f"index '{index_name}' already exists")
            return self

        self._es_client.indices.create(
            index=index_name,
        )
        self.logger.debug(f"index '{index_name}' created")
        return self

    def delete_index(self, index_name: str) -> OpenSearchConsumer:
        """delete the index if it exists

        Args:
            index_name (str): index name

        Returns:
            OpenSearchConsumer: self
        """        
        if self._es_client is None:
            self.logger.warning(f"_es_client is None - No index '{index_name}' \
                 deleted - Please run opensearch_connect()")
            return self
        if not self._es_client.indices.exists(index=index_name):
            self.logger.info(f"index '{index_name}' doesn't exists")
            return self

        self._es_client.indices.delete(
            index=index_name,
        )
        self.logger.debug(f"index '{index_name}' deleted")
        return self

    def close(self) -> OpenSearchConsumer:
        """close database connection and kafka stream consumer

        Returns:
            OpenSearchConsumer: self
        """        
        if self._es_client is None:
            self.logger.info(f"_es_client is None - do nothing")
            return self
        self._es_client.close()
        self._es_client = None
        super().close()
        return self

    @staticmethod
    def _bulk_json_data(json_list: list, _index: str, doc_type: str) -> Dict:
        """transform json the send i to opensearch database
        see: https://kb.objectrocket.com/elasticsearch/how-to-use-python-helpers-to-bulk-load-data-into-an-elasticsearch-index


        Args:
            json_list (list): list of jsons
            _index (str): opensearch index
            doc_type (str): document type

        Returns:
            Dict: _description_

        Yields:
            Iterator[Dict]: _description_
        """
        for doc in json_list:
            # use a `yield` generator so that the data
            # isn't loaded into memory

            if '{"index"' not in doc:
                yield {
                    "_index": _index,
                    "_type": doc_type,
                    "_id": uuid.uuid4(),
                    "_source": doc
                }

    def send_to_index(self, message:str, index_name:str) -> OpenSearchConsumer:
        """send a message to the database at the given index

        Args:
            message (str): message to insert
            index_name (str): index name in the database

        Returns:
            OpenSearchConsumer: self
        """
        if self._es_client is None:
            self.logger.warning(f"_es_client is None - Please run opensearch_connect()")
            return self
        if not self._es_client.indices.exists(index=index_name):
            self.logger.info(f"index '{index_name}' doesn't exists")
            return self
        msg_json = json.loads(message.decode('utf-8'))

        response = helpers.bulk(
            self._es_client,
            OpenSearchConsumer._bulk_json_data(
                json_list=[msg_json],
                _index=index_name,
                doc_type="tbd",
            )
        )
        self.logger.info(response)
        return self

    def loop_for_messages(self, index_name: str, timeout=10) -> None:
        try:
            while True:
                msg = self.read_from_topic(timeout=timeout)
                if msg is None:
                    break
                try:
                    self.send_to_index(msg, index_name)
                except json.JSONDecodeError:
                    logging.warning(f"Decoding error for {msg=}")

        except KeyboardInterrupt:
            self.logger.info('%% Aborted by user\n')
        except Exception as ex:
            self.logger.error(f'%% Unexpected Exception {type(ex)}: {ex}\n')
        finally:
            # Close down consumer to commit final offsets.
            self.logger.info("Shutdown consumer")
            self.close()

def main():
    """default main for showcasing, testing
    """    
    logging.basicConfig(level=logging.WARNING, \
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # create opensearch client
    # see: https://docs.bonsai.io/article/102-python
    fn = "./credentials.yaml"
    with open(fn, 'r') as fh:
        try:
            credentials = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            logging.error(f"Error during '{fn}' import {exc}")
            return
    es_header=[{
        'host': credentials["bonsai"]["host"],
        'port': credentials["bonsai"]["port"],
        'use_ssl': True,
        'http_auth': (credentials["bonsai"]["auth"]["key"], credentials["bonsai"]["auth"]["secret"]),
    }]
   
    # create Kafka-client
    # Configuration see: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    params = {
        'bootstrap.servers': 'localhost:9092',
        'ssl.key.location': '/Users/sgrehl/.ssh',
    }
    params_consumer = {
        'group.id': 'my-python-opensearch-application',
        'auto.offset.reset': 'latest',  # one of: none, earliest, latest
        'partition.assignment.strategy': 'roundrobin',  # one of: range,roundrobin - Coorperative strategies (in contrast to eager) not found in setings
        # 'group.instance.id' static group memebership, stops reassigment for given time
        'auto.commit.interval.ms': 5000, # if timout is reached consumer informs broker to set offset
    }
    consumer: OpenSearchConsumer = OpenSearchConsumer(
        config={**params, **params_consumer},
        es_header=es_header,
        )
    # get wikimedia mapping 
    # mapping_url = "https://en.wikipedia.org/w/api.php?action=cirrus-mapping-dump&format=json&formatversion=2"
    # mapping = {}
    # import urllib.request
    # import json
    # with urllib.request.urlopen(mapping_url) as url:
    #     mapping = json.loads(url.read().decode())
    try:
        consumer.opensearch_connect()
        consumer.delete_index("wikiemdia").create_index("wikimedia")
        consumer.subscribe(["wikimedia.recentchange"])
        consumer.loop_for_messages(index_name="wikimedia")
    except Exception as e:
        logging.error(f"error during index creation {e}")
    finally:
        consumer.close()
    # main code logic

    # close things

if __name__ == '__main__':
    main()