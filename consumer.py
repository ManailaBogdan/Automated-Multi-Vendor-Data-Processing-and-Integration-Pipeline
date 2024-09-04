'''Module for processing data from Rabbitmq queue'''
import sys
import time
import pika
import config
from log_init import logger

class Consumer:
    ''''
    Class for taking data from RabbitMQ
    and adding it to a Data Base

    Attributes:
    -----------
    queue: str
        Name of the queue from which to take data
    rabbit_conn: BlockingConnection object
        Object to manage the rabbit connection
    channel: Channel object
        Object to perform rabbit messaging operations


    Methods:
    --------
    rabbit_connection(attempt: int) -> None:
        Try to start rabbit connections for all the threads
    db_connection(attempt: int) -> None:
        Try to start DB server connections
    callback(ch, method, proprietes, body) -> None:
        Function to execute when retrieving a rabbit message
    task() -> None:
        Start consuming messages
    close_rabbit() -> None
        Stop rabbit connection
    close_db() -> None:
        Stop DB server connection
    close() -> None:
        Gracefully shut down consumer
    '''
    def __init__(self):
        '''Initialize consumer object'''
        self.queue = None
        # start Rabbitmq connection and chanel
        self.rabbit_connection(0)

        # start DB connection
        self.db_connection(0)


    def rabbit_connection(self, attempt):
        '''Start rabbitmq connection'''
        if attempt <= config.connection_retries:
            try:

                self.rabbit_conn = pika.BlockingConnection(
                    pika.ConnectionParameters(**config.rabbit_connection)
                )
                self.channel = self.rabbit_conn.channel()
                self.channel.exchange_declare(**config.rabbit_exchange)
                logger.info("RABBIT CONNECTION ON")

            except Exception as e:
                logger.error(e)
                attempt += 1
                time.sleep(attempt)
                logger.debug(f"ATTEMPT {attempt} TO ESTABLISH RABBIT CONNECTION")
                self.rabbit_connection(attempt)
        else:
            logger.error("CAN'T ESTABLISH RABBIT CONNECTION")
            self.close_db()
            sys.exit(1)


    def db_connection(self, attempt):
        '''Start Data Base connection'''


    def callback(self, ch, method, proprietes, body):
        '''
        Add data to DataBase and send ACK to Rabbitmq
        Args:
            ch: channel
            method: delivery informatio
            proprietes: message proprietes
            body: data
        '''


    def task(self):
        '''Start processing data from queue'''
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue,
                                       on_message_callback=self.callback,
                                       auto_ack=False)
            self.channel.start_consuming()
        except Exception as e:
            if not self.rabbit_conn.is_open:
                self.rabbit_connection(0)
                self.task()
            else:
                logger.error(e)

    def get_day(self, date: str):
        return date.split(' ')[0].replace('-', '_').replace('-', '_')

    def close_db(self):
        '''Stop Data Base connection'''

    def close_rabbit(self):
        '''Stop rabbit connection'''
        if self.rabbit_conn.is_open:
            self.channel.stop_consuming()
            self.rabbit_conn.close()
            logger.info("RABBIT CONNECTION OFF")

    def close(self):
        '''Graceful shutdown'''
        self.close_db()
        self.close_rabbit()
        sys.exit(0)
