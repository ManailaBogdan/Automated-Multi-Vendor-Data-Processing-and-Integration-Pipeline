'''Module for processing data from Rabbitmq queue'''
import sys
import time
import json
import pika
import mysql.connector
from consumer import Consumer
import config
from log_init import logger

class ConsumerSQL(Consumer):
    ''''
    Class for taking data from RabbitMQ
    and adding it to MySQL server

    Attributes:
    -----------
    sql_conn: MYSQLConnection object
        Object to manage MYSQL connection
    cursor: Cursor object
        Object to execute MYSQL statements

    Methods:
    --------
    db_connection(attempt: int) -> None:
        Try and retry to start MySQL server connections
    manage_db_error(e: Exception, body: [dict]) -> int
        Handle MySQL related exception
    malformed_msg(body: [dict]) -> None
        Handle a malformed message
    close_rabbit() -> None
        Stop rabbit connection
    close_db() -> None:
        Stop MYSQL server connection
    '''

    def __init__(self):
        '''Initialize consumer object'''
        super().__init__()
        self.table_prefix = None
        self.table = None

    def db_connection(self, attempt):
        '''Start MYSQL db connection'''
        if attempt <= config.connection_retries:
            try:
                time.sleep(attempt)
                self.sql_conn = mysql.connector.connect(**config.sql_config)
                self.cursor = self.sql_conn.cursor()
                logger.info("MYSQL CONNECTION ON")
            except Exception as e:
                logger.error(e)
                attempt += 1
                logger.debug(f"ATTEMPT {attempt} TO ESTABLISH MYSQL CONNECTION")
                self.db_connection(attempt)
        else:
            logger.error("CAN'T ESTABLISH MYSQL CONNECTION")
            self.close_rabbit()
            sys.exit(1)

    def manage_db_error(self, e, body = []):
        '''
            Handle MySQL related exception
            Args:
                e: Exception obj
                body: data
            Return:
                1: redo the operations that threw the exception
                0: continue
        '''
        
        if not self.sql_conn.is_connected():  # Server unreachable
            self.db_connection(0)
            return 1

        error_message = str(e)
        errno = int(error_message.split(' ', maxsplit=1)[0])
        if errno == 1062:  # Duplicate message
            column_name = error_message.split("'")[3]
            if 'md5' in column_name:
                logger.error('MD5 DOUBLE')
                return 0

        if errno == 1146:
            logger.error("TABLE DOESN'T EXIST")
            self.gen_table()
            return 1

        if errno == 1050:
            logger.error("TABLE ALREADY EXISTS")
            return 0

        # Other error
        logger.debug("MANAGE MALFORMED CHUNK")
        self.malformed_msg(body)
        return 0

    def malformed_msg(self, body):
        '''
        Send the message line by line, omitting the malformed ones
        Args:
            body: data
        '''
        while True:
            try:
                self.sql_conn.rollback()
                self.sql_conn.start_transaction()
                break
            except Exception as e:
                logger.error(e)
                if not self.sql_conn.is_connected():
                    self.db_connection(0)

        logger.info('TRANSACTION STARTED')
        for line in body:
            columns = ','.join(line.keys())
            values = ', '.join(['%s'] * len(line))
            insert = f"INSERT INTO {self.table} ({columns}) VALUES ({values});"
            while True:
                try:
                    self.cursor.execute(insert, list(line.values()))

                    break
                except Exception as e:
                    if not self.sql_conn.is_connected():
                        self.db_connection(0)
                    else:
                        logger.error(f"LINE {line} gives error {e}")
                        break

        while True:
            try:
                self.sql_conn.commit()
                break
            except Exception as e:
                logger.error(e)
                if not self.sql_conn.is_connected():
                    self.db_connection(0)
        logger.info("GOOD LINES COMMITED")

    def callback(self, ch, method, proprietes, body):
        '''
        Add RabbitMQ message to MySQL Alcatel table
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
            line['md5'] = self.gen_md5(line)

        month = self.get_day(body[0]['start_date'])
        self.table = self.table_prefix + '_' + month

        columns = ','.join(body[0].keys())
        values = ', '.join(
            "({})".format(', '.join('%s' for _ in line.values()))
            for line in body
        )
        insert = f"INSERT INTO {self.table} ({columns}) VALUES {values}"

        values_tuple = tuple(val for line in body for val in line.values())
        while True:
            try:
                self.sql_conn.rollback()
                self.cursor.execute(insert, values_tuple)
                self.sql_conn.commit()
                break
            except Exception as e:
                if self.manage_db_error(e, body) == 0:
                    break


        self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def gen_md5(self, line):
        pass

    def gen_table(self, name):
        pass

    def close_db(self):
        '''Stop MYSQL connection'''
        if self.sql_conn.is_connected():
            self.cursor.close()
            self.sql_conn.close()
            logger.info("MYSQL CONNECTION OFF")
