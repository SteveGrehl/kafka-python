#!/usr/bin/env python

import logging
import json
from confluent_kafka import KafkaException
from kafka_tutorial import BasicKafkaConsumer
import numpy as np


class OpenSearchConsumer(BasicKafkaConsumer):

    def __init__(self, config: dict, group_id=None) -> None:
        super().__init__(config, group_id)