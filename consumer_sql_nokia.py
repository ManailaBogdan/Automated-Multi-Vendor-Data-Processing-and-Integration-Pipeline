'''Module for processing data from Rabbitmq queue'''
import signal
import json
import hashlib
import config
from log_init import logger
from consumer_sql import ConsumerSQL

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



class Nokia(ConsumerSQL):
    '''
    Class for taking data from RabbitMQ Nokia queue
    and adding it to MySQL Nokia table

    Attributes:
    -----------
    channel: Channel object
        Object to perform rabbit messaging operations
    queue: str
        Name of the queue from which to take date
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
        Send the message line by line, omitting the malformed ones
    callback(ch, method, proprietes, body) -> None:
        Add RabbitMQ message to MySQL Nokia table
    '''

    def __init__(self):
        '''Start RabbitMQ and MySQL connections'''
        super().__init__()

        result = self.channel.queue_declare(**config.nokia_sql_queue)
        self.queue = result.method.queue
        self.channel.queue_bind(**config.nokia_sql_bind)

        self.table_prefix = config.nokia_table

    def gen_md5(self, line):
        unique_string = ""
        for field in config.md5_nokia:
            unique_string += f"{line[field]}"
        md5_field = hashlib.md5(unique_string.encode()).hexdigest()
        return md5_field


    def gen_table(self):
        command = f"""CREATE TABLE {self.table} (
                    {config.nokia_table_fields}
                );"""

        while True:
            try:
                self.cursor.execute(command)
            except Exception as e:
                if self.manage_db_error(e) == 0:
                    break





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
