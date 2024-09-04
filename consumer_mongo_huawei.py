'''
Module for handling signals and
taking data from RabbitMQ Huawei queue
and adding it to the Mongo Huawei collection for that month
'''
import signal
import hashlib
import config
from log_init import logger
from consumer_mongo import Consumer_Mongo

consumer = None # pylint: disable=invalid-name

def signal_handler(signum, frame):# pylint: disable=unused-argument
    '''
    Graceful shutdown on SIGINT
    Args:
        signum: signal number
        frame: program state object
    '''
    consumer.close()

def signal_init():
    '''Binding signal_handler to SIGINT'''
    signal.signal(signal.SIGINT, signal_handler)

class Huawei(Consumer_Mongo):
    '''
    Consumer Mongo for Huawei vendor

    Attributes:
    -----------
    channel: Channel object
        Object to perform rabbit messaging operations
    queue: str
        Name of the queue from which to take data
    collection_prefix: str
        Prefix on collection name to determine vendor

    Methods:
    --------
    gen_md5(line: dict) -> str:
        Generate hash from the specific fields of the data
    '''
    def __init__(self):
        '''Initialize consumer object'''
        super().__init__()

        self.collection_prefix = config.huawei_collection

        # create queue
        result = self.channel.queue_declare(**config.huawei_mongo_queue)
        self.queue = result.method.queue
        self.channel.queue_bind(**config.huawei_mongo_bind)

    def gen_md5(self, line):
        '''
        Generate hash from the specific fields of the data
        Args:
            line: dictionary of data
        Returns:
            md5_field: string to uniquely represent the data
        '''
        unique_string = ""
        for field in config.md5_huawei:
            unique_string += f"{line[field]}"
        md5_field = hashlib.md5(unique_string.encode()).hexdigest()
        return md5_field

if __name__ == "__main__":
    signal_init()
    consumer = Huawei()

    try:
        consumer.task()
    except Exception as e:
        try:
            logger.error(e)

        except Exception as e:
            logger.error(e)
