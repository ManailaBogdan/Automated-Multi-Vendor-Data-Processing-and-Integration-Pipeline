'''
Module for handling signals and
taking data from RabbitMQ Nokia queue
and adding it to the Mongo Nokia collection for that month
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

class Nokia(Consumer_Mongo):
    '''Consumer for Nokia vendor'''
    def __init__(self):
        '''Initialize consumer object'''
        super().__init__()

        self.collection_prefix = config.nokia_collection

        #create queue
        result = self.channel.queue_declare(**config.nokia_mongo_queue)
        self.queue = result.method.queue
        self.channel.queue_bind(**config.nokia_mongo_bind)

    def gen_md5(self, line):
        '''
        Generate hash from the specific fields of the data
        Args:
            line: dictionary of data
        Returns:
            md5_field: string to uniquely represent the data
        '''
        unique_string = ""
        for field in config.md5_nokia:
            unique_string += f"{line[field]}"
        md5_field = hashlib.md5(unique_string.encode()).hexdigest()
        return md5_field

if __name__ == "__main__":
    signal_init()
    consumer = Nokia()

    try:
        consumer.task()
    except Exception as e:
        try:
            logger.error(e)

        except Exception as e:
            logger.error(e)
