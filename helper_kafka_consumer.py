import time

from my_logger import *
from confluent_kafka import Consumer, KafkaError, TopicPartition


logger = logging.getLogger('root')


class KafkaConsumer(object):
    def __init__(self, **kwargs):
        self.c = Consumer({
            'bootstrap.servers': kwargs['services'],
            'group.id': kwargs['groupid'],
            'socket.timeout.ms': '15000',
            'session.timeout.ms': 10000,
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'enable.auto.commit': "false"
        })
        self.c.subscribe([kwargs['topic']])

    def __del__(self):
        self.c.close()
