'''
Module taking data from RabbitMQ queue
and adding it to the Mongo collection for that month
'''
import sys
import json
import time
import datetime
import pika
import pymongo
from consumer import Consumer
import config
from log_init import logger

class Consumer_Mongo(Consumer):
    ''''
    Class for taking data from RabbitMQ
    and adding it to MongoDB server

    Attributes:
    -----------
    collection_prefix: str
        Prefix on collection name to determine vendor
    collection: pymongo collection object
        Object to execute Mongo statements on the collection
    queue: str
        Name of the queue from which to take date
    rabbit_conn: BlockingConnection object
        Object to manage the rabbit connection
    channel: Channel object
        Object to perform rabbit messaging operations
    mongo_conn: MongoClient object
        Object to manage Mongo connection
    mongo_db: pymongo database object
        Object to execute Mongo statements on the db

    Methods:
    --------
    rabbit_connection(attempt: int) -> None:
        Try to start rabbit connections for all the threads
    db_connection(attempt: int) -> None:
        Try to start Mongo server connections
    callback(ch, method, proprietes, body) -> None:
        Function to execute when retrieving a rabbit message
    task() -> None:
        Start consuming messages
    close_rabbit() -> None
        Stop rabbit connection
    close_sql() -> None:
        Stop MYSQL server connection
    close() -> None:
        Gracefully shut down consumer
    '''
    def __init__(self):
        '''Initialize consumer object'''
        super().__init__()
        self.collection_prefix = None
        self.collection = None

    def db_connection(self, attempt):
        '''
        Try to start Mongo server connections
        Arg:
            attempt: number of the connection retry
        '''
        if attempt <= config.connection_retries:
            try:
                time.sleep(attempt)
                self.mongo_conn = pymongo.MongoClient(**config.mongo_config)
                self.mongo_db = self.mongo_conn[config.mongo_db_name]

                logger.info("MONGO CONNECTION ON")
            except Exception as e:
                logger.error("NO MONGO CONNECTION")
                attempt += 1
                logger.debug(f"ATTEMPT {attempt} TO ESTABLISH MONGO CONNECTION")
                self.db_connection(attempt)
        else:
            logger.error("CAN'T ESTABLISH MONGO CONNECTION")
            self.close_rabbit()
            sys.exit(1)

    def manage_db_error(self, e, body):
        '''
            Handle MONGO related exception
            Args:
                e: Exception obj
                body: data
        '''
        
        try:
            _ = self.mongo_conn.server_info()
        except Exception as e:
            self.db_connection(0)
            return 1

        error_message = str(e)

        if 'md5_1 dup key' in error_message:
            logger.error('MD5 DOUBLE')
            return 0

        if '_id_ dup key' in error_message:
            logger.error('ID DOUBLE')
            return 0

    def callback(self, ch, method, proprietes, body):
        '''
        Add data to DataBase and send ACK to Rabbitmq
        Args:
            ch: channel
            method: delivery informatio
            proprietes: message proprietes
            body: data
        '''
        body = json.loads(body)
        if body == []:
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
            return
        for line in body:
            line['insert_date'] = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            line['md5'] = self.gen_md5(line)

        month = self.get_day(body[0]['start_date'])

        while True:
            try:
                self.collection = self.mongo_db[self.collection_prefix + '_' + month]
                self.collection.create_index([('md5', pymongo.ASCENDING)], unique=True)

                self.collection.insert_many(body, ordered=False)


                break
            except Exception as e:

                if self.manage_db_error(e, body) == 0:
                    break

        self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def gen_md5(self, line):
        pass

    def close_db(self):
        '''Stop MONGO connection'''
        try:
            _ = self.mongo_conn.server_info()
        except Exception as e:
            logger.info('MONGO ALREADY OFF')
            return


        self.mongo_conn.close()

        logger.info('MONGO CONNECTION OFF')


