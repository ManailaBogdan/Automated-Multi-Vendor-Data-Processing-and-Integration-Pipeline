'''Module for putting vendor data on Rabbit queue'''
import sys
import time
import signal
import os
import json
import shutil
import importlib
import csv
from threading import Event, Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import pika
import config
from log_init import logger

producer = None # pylint: disable=invalid-name

def signal_handler(signum, frame): # pylint: disable=unused-argument
    '''
    Graceful shutdown on SIGINT
    Args:
        signum: signal number
        frame: program state object
    '''
    if signum == signal.SIGINT:
        producer.shutdown.set()

def signal_init():
    '''Binding signal_handler to SIGINT'''
    signal.signal(signal.SIGINT, signal_handler)

class Producer:
    '''
    Class for checking and parsing data from csv files
    and giving it to RabbitMQ exchange

    Attributes:
    -----------
    file_type: str
        Name of the vendor from which to take files
    proc_id: int
        Number for evenly dividing files between producers
    mod: int
        Number of producers started
    module: bool/Module object
        Module object containing data for unfinished file when processing it,
        False otherwise
    unfinished_file: str
        Name of the file being processed
    row: list of int
        List of the last row processed by each thread
    destination: str
        Name of the folder to which to send a file when fully processed
    shutdown: Event
        Event to notify the producer to gracefully shut down
    connection: list of BlockingConnection objects
        List of objects to manage the rabbit connection for each thread
    channel: list of Channel objects
        List of objects to perform rabbit messaging operations for each thread

    Methods:
    --------
    rabbit_connection(attempt: int) -> None:
        Try to start rabbit connections for all the threads
    process(file: str, rows_done: int, id: int) -> None:
        Process vendor file and send data to queue
    task() -> None:
        Sends files to be processed
    move_processed_file(file: str) -> None:
        Move file to destination folder
        and reset file attributes
    get_file_timestamp(file: str) -> int:
        Gets file timestamp from file name
    get_file_type(file: str) -> str:
        Gets the name of the vendor that producessed the file
    save() -> None:
        Saving unfinished file name, rows done by threads
        and file status to dedicated folder
    check_alcatel(keys: list of str, line: dict) -> bool/dict:
        Check line viability for alcatel file
        and return dict with proper types
    check_nokia(keys: list of str, line: dict) -> bool/dict:
        Check line viability for nokia file
        and return dict with proper types
    check_huawei(keys: list of str, line: dict) -> bool/dict:
        Check line viability for huawei file
        and return dict with proper types
    close() -> None:
        Closes Rabbitmq connection
    '''
    def __init__(self, file_type, proc_id, mod):
        '''Initialize producer object'''

        self.file_type = file_type
        self.id = int(proc_id)
        self.mod = int(mod)

        self.module = False

        self.unfinished_file = ""
        self.row = [0 for _ in range(config.thread_number)]
        self.destination = config.destination_success_path

        self.shutdown = Event()
        self.lock = Lock()

        self.rabbit_connection(0)
        # logger.debug(f"PRODUCER STARTED: FILE {file_type} - ID {proc_id} - MOD {mod}")

    def rabbit_connection(self, attempt):
        '''Try to start rabbit connections for all the threads'''
        if attempt <= config.connection_retries:
            try:
                # start Rabbitmq connections and channels for the threads
                self.connection = list(range(config.thread_number))
                self.channel = list(range(config.thread_number))

                for i in range(config.thread_number):
                    self.connection[i] = pika.BlockingConnection(
                        pika.ConnectionParameters(**config.rabbit_connection)
                    )
                    self.channel[i] = self.connection[i].channel()

                    self.channel[i].exchange_declare(**config.rabbit_exchange)
                # logger.info(f"RABBIT CONNECTIONS ON")
            except Exception as e:
                # wait and restart connection
                attempt += 1
                time.sleep(attempt)
                logger.error(e)
                logger.debug(f"ATTEMPT {attempt} TO ESTABLISH RABBIT CONNECTION")
                self.rabbit_connection(attempt)
        else:
            # close program
            logger.error("CAN'T ESTABLISH RABBIT CONNECTION")
            self.save()
            sys.exit(1)

    def same_day(self, d1: str, d2: str):
        '''
        Check if 2 datetime strings are in the same day
        Args:
            d1: firs date
            d2: second date
        Return:
            True/False
        '''
        if self.file_type == 'alcatel':
            d1 = d1.split(',')[config.alcatel_start_date_pos].split(' ')[0]
            d2 = d2.split(',')[config.alcatel_start_date_pos].split(' ')[0]
        elif self.file_type == 'nokia':
            d1 = d1.split(',')[config.nokia_start_date_pos].split(' ')[0]
            d2 = d2.split(',')[config.nokia_start_date_pos].split(' ')[0]
        elif self.file_type == 'huawei':
            d1 = d1.split(',')[config.huawei_start_date_pos].split(' ')[0]
            d2 = d2.split(',')[config.huawei_start_date_pos].split(' ')[0]

        if d1 == d2:
            return True
        return False

    def process(self, file: str, rows_done: int, thread_id: int):
        '''
        Process vendor file and send data to queue
        Sends line with row % number of threads = id
        Args:
            file: name of the file to be processed
            rows_done: rows already sent to consumer
            id: thread id
        '''

        # logger.debug(f"THREAD {thread_id} HERE {rows_done}")

        #file info
        self.row[thread_id] = thread_id
        self.unfinished_file = file

        # open data file
        try:
            f = open(config.destination_work_path + file, 'r',
                     encoding='utf-8')
        except Exception as e:
            logger.error(e)
            self.shutdown.set()
            return

        line = f.readline()
        # csv fields
        keys = line[:-1].split(',')


        last_line = [] # unprocessed line
        i = -1 # line index

        # sending data chunks loop
        while True:

            # make chunk
            lines = last_line
            last_line = []
            for _ in range(config.chunk_size * config.thread_number):
                line = f.readline()
                i += 1

                if not line:
                    break

                if i % config.thread_number == thread_id:
                    if lines != []:
                        if self.same_day(lines[-1], line):
                            lines.append(line)
                        else:
                            last_line = [line]
                            break
                    else:
                        lines.append(line)


            if len(lines) == 0:
                break

            lines = csv.DictReader(lines, fieldnames=keys)

            # send chunk
            if not self.shutdown.is_set():
                msg = []
                for line in lines:
                    cp = line
                    if self.row[thread_id] >= rows_done:

                        # gen dict by vendor and checks for errors
                        if 'alcatel' in file:
                            line = self.check_alcatel(keys, line)
                        elif 'nokia' in file:
                            line = self.check_nokia(keys, line)
                        elif 'huawei' in file:
                            line = self.check_huawei(keys, line)

                        if isinstance(line, dict):
                            msg.append(line)
                        else :
                            logger.error(f"ERROR: LINE {self.row[thread_id]}: {cp}")
                            with self.lock:
                                self.destination = config.destination_error_path

                    self.row[thread_id] += config.thread_number

                if msg == []:
                    continue

                #send chunck of data
                if self.row[thread_id] > rows_done:
                    json_d = json.dumps(msg)
                    while True:
                        try:
                            self.channel[thread_id].basic_publish(
                                exchange=config.rabbit_exchange['exchange'],
                                routing_key=f'vendor.{self.file_type}',
                                body=json_d,
                                properties=pika.BasicProperties(delivery_mode=2))

                            break
                        except Exception as e:
                            logger.error(e)
                            with self.lock:
                                if not self.connection[thread_id].is_open:
                                    self.rabbit_connection(0)


                    # first_row = self.row[thread_id] - config.chunk_size*config.thread_number
                    # if first_row < 0:
                    #     first_row = thread_id

                    # logger.debug(f"""SENT FILE: {file} - LINES: {first_row} {
                    #    self.row[thread_id]} {thread_id}""")
            else:
                break

        f.close()
        if self.shutdown.is_set():
            return

    def task(self):
        ''''Sends files to be processed'''
        ct = 0 #files processed or in progress

        # process unfinushed files
        files = os.listdir(config.path_unfinished_files)
        for file in files:
            if os.path.isfile(config.path_unfinished_files + file):
                fields = file.split('_')
                file_id = fields[0]
                file_type = fields[1]

                if int(file_id) % self.mod == self.id and file_type == self.file_type:
                    self.module = importlib.import_module(
                        config.folder_unfinished_files + "." + file.split('.')[0])

                    if self.module.MALFORMED:
                        self.destination = config.destination_error_path
                    else:
                        self.destination = config.destination_success_path

                    # start threads to process file
                    with ThreadPoolExecutor() as ex:
                        futures = [ex.submit(self.process, self.module.UNFINISHED_FILE,
                                             self.module.ROW[i], i)
                                   for i in range(config.thread_number)]
                        for future in futures:
                            _ = future.result()
                        ct += 1

                    # move file to destination folder
                    if not self.shutdown.is_set():
                        self.move_processed_file(self.module.UNFINISHED_FILE)

                    # delete data about unfinished file
                    os.remove(config.path_unfinished_files + file)


        # process files
        self.module = False
        files = os.listdir(os.path.abspath(config.source_path))
        # sort files by age
        files.sort(key = self.get_file_timestamp)
        for file in files:
            if self.shutdown.is_set():
                break

            if (self.get_file_timestamp(file) % self.mod == self.id and
                self.file_type == self.get_file_type(file)):
                # send file to work folder
                shutil.move(config.source_path + file, config.destination_work_path + file)

                self.destination = config.destination_success_path
                # start threads to process file
                with ThreadPoolExecutor() as ex:
                    futures = [ex.submit(self.process, file, -1, i)
                                for i in range(config.thread_number)]
                    for future in as_completed(futures):
                        _ = future.result()
                    ct += 1

                if self.shutdown.is_set():
                    break

                # move file to destination folder
                self.move_processed_file(file)

        # wait a second if there's no work
        if ct == 0:
            time.sleep(1)
            # logger.info("NO FILES")
            self.close()
            sys.exit(1)

        # save progress
        self.save()
        # logger.info("PROGRESS SAVED")

        # close connection
        self.close()

    def move_processed_file(self, file):
        '''
        Move file to destination folder
        and reset file attributes
        Args:
            file: name of the file
        '''
        shutil.move(config.destination_work_path + file, self.destination + file)
        self.unfinished_file = ""
        self.row = [0 for _ in range(config.thread_number)]
        self.destination = config.destination_success_path

        logger.info(f"FILE {file} SENT SUCCESSFULLY")

    def get_file_timestamp(self, file):
        '''
        Gets file timestamp from file name
        '''
        fields = file.split('_')
        return int(int(fields[2]) / config.file_frequency)

    def get_file_type(self, file):
        '''
        Gets the name of the vendor that producessed the file
        '''
        fields = file.split('_')
        return fields[1]

    def save(self):
        '''
        Saving unfinished file name, rows done by threads
        and file status to dedicated folder
        '''
        if self.unfinished_file != "":
            if self.module:
                if self.module.UNFINISHED_FILE == self.unfinished_file:
                    for i in range(config.thread_number):
                        if self.row[i] < int(self.module.ROW[i]):
                            self.row[i] = int(self.module.ROW[i])


            with open(config.path_unfinished_files +
                      str(self.id) + "_" +
                      self.file_type + "_file.py", 'w',
                      encoding='utf-8') as file:

                file.write(f"UNFINISHED_FILE = \"{self.unfinished_file}\"\n")
                file.write(f"ROW = {self.row}\n")
                if self.destination == config.destination_success_path:
                    file.write("MALFORMED = False\n")
                else:
                    file.write("MALFORMED = True\n")

    def check_alcatel(self, keys, line):
        '''
        Check line viability for alcatel file
        and return dict with proper types
        Args:
            keys: list of name of keys
            line: dict
        Return:
            False: if line malformed
            Dict: if line is ok
        '''
        types = config.fields_types_alcatel
        cfg = config.cfg_alcatel

        if len(keys) != cfg['field_count'] or len(line) != cfg['field_count']:
            return False

        for key, type in zip(keys, types):
            try:
                val = type(line[key])
            except:
                line = False
                break
            line[key] = val

            if key == 'src' and val != 'alcatel':
                line = False
                break
            if key == 'idapp' and (len(val) < cfg['low_id_len'] or len(val) > cfg['high_id_len']):
                line = False
                break
            if key == 't1' and (val < cfg['low_t1'] or val > cfg['high_t1']):
                line = False
                break
            if key == 'ncu' and (val < cfg['low_ncu'] or val > cfg['high_ncu']):
                line = False
                break
            if key == 'start_date' and (val < cfg['low_date'] or val > cfg['high_date']):
                line = False
                break
            if (key == 'ano' and
                  (val[:len(cfg['ano_start'])] !=  cfg['ano_start'] or
                   len(val) != cfg['ano_len'])
                  ):
                line = False
                break
            if key == 'duration' and  (val < cfg['low_duration'] or val > cfg['high_duration']):
                line = False
                break
            if (key == 'bno' and
                  (val[:len(cfg['bno_start'])] !=  cfg['bno_start']
                   or len(val) != cfg['bno_len'])
                  ):
                line = False
                break

        return line

    def check_nokia(self, keys, line):
        '''
        Check line viability for nokia file
        Args:
            keys: list of name of keys
            vals: list of string values
        Return:
            False: if line malformed
            Dict: if line is ok
        '''
        types = config.fields_types_nokia
        cfg = config.cfg_nokia

        if len(keys) != cfg['field_count'] or len(line) != cfg['field_count']:
            return False

        for key, type in zip(keys, types):
            try:
                val = type(line[key])
            except:
                line = False
                break
            line[key] = val

            if key == 'src' and val != 'nokia':
                logger.error(f"SRC WRONG: {val} SHOULD BE nokia")
                line = False
                break
            if (key == 'record_type' and
                  (val < cfg['low_record_type'] or
                   val > cfg['high_record_type'])
                  ):
                logger.error(f"RECORD TYPE WRONG: {val} out of bounds")
                line = False
                break
            if key == 'start_date' and (val < cfg['low_date'] or val > cfg['high_date']):
                logger.error(f"START DATE WRONG: {val} out of bounds")
                line = False
                break
            if key == 'duration' and  (val < cfg['low_duration'] or val > cfg['high_duration']):
                logger.error(f"DURATION WRONG: {val} out of bounds")
                line = False
                break
            if (key == 'record_length' and
                  (val < cfg['low_record_len'] or
                   val > cfg['high_record_len'])
                  ):
                logger.error(f"RECORD LENGTH: {val} out of bounds")
                line = False
                break
            if (key == 'ano' and
                  (val[:len(cfg['ano_start'])] !=  cfg['ano_start'] or
                   len(val) != cfg['ano_len'])
                  ):
                logger.error(f"ANO WRONG: {val}")
                line = False
                break
            if (key == 'record_number' and
                  (val < cfg['low_record_num'] or
                   val > cfg['high_record_num'])
                  ):
                logger.error(f"RECORD NUMBER WRONG: {val} out of bounds")
                line = False
                break
            if key == 'call_reference' and len(val) != cfg['call_ref_len']:
                line = False
                break
            if (key == 'bno' and
                  (val[:len(cfg['bno_start'])] !=  cfg['bno_start'] or
                   len(val) != cfg['bno_len'])
                  ):
                logger.error(f"BNO WRONG: {val}")
                line = False
                break

        return line

    def check_huawei(self, keys, line):
        '''
        Check line viability for huawei file
        Args:
            keys: list of name of keys
            vals: list of string values
        Return:
            False: if line malformed
            Dict: if line is ok
        '''
        types = config.fields_types_huawei
        cfg = config.cfg_huawei

        if len(keys) != cfg['field_count'] or len(line) != cfg['field_count']:
            return False

        for key, type in zip(keys, types):
            try:
                val = type(line[key])
            except:
                line = False
                break
            line[key] = val

            if key == 'src' and val != 'huawei':
                line = False
                break
            if (key == 'record_type' and
                  (val < cfg['low_record_type'] or
                   val > cfg['high_record_type'])
                  ):
                line = False
                break
            if (key == 'imsi' and
                  (val[:len(cfg['imsi_start'])] !=  cfg['imsi_start'] or
                   len(val) != cfg['imsi_len'])
                  ):
                line = False
                break
            if (key == 'msisdn' and
                  (val[:len(cfg['msisdn_start'])] !=  cfg['msisdn_start'] or
                   len(val) != cfg['msisdn_len'])
                  ):
                line = False
                break
            if key == 'start_date' and (val < cfg['low_date'] or val > cfg['high_date']):
                line = False
                break
            if key in ('pGWAddress', 'pdp_address'):
                fields = val.split('.')
                if len(fields) != 4:
                    line = False
                    break
                for field in fields:
                    if int(field) < cfg['low_ip_addr'] or int(field) > cfg['high_ip_addr']:
                        line = False
                        break
            if key == 'chargingID' and (val < cfg['low_charg_id'] or val > cfg['high_charg_id']):
                line = False
                break
            if key == 'apn' and val not in cfg['apn']:
                line = False
                break
            if (key == 'datavolumeuplink' and
                  (val < cfg['low_up_link'] or
                   val > cfg['high_up_link'])
                  ):
                line = False
                break
            if (key == 'datavolumedownlink' and
                  (val < cfg['low_down_link'] or
                   val > cfg['high_down_link'])):
                line = False
                break
            if key == 'duration' and  (val < cfg['low_duration'] or val > cfg['high_duration']):
                line = False
                break
            if key == 'rat_type' and  (val < cfg['low_rat_type'] or val > cfg['high_rat_type']):
                line = False
                break

        return line

    def close(self):
        '''Closes Rabbitmq connection'''
        for i in range(config.thread_number):
            self.connection[i].close()
        # logger.info("CONNECTIONS OFF")

if __name__ == "__main__":
    signal_init()

    if len(sys.argv) > 3:
        try:
            producer = Producer(sys.argv[1], sys.argv[2], sys.argv[3])
        except Exception as e:
            logger.error(e)
            sys.exit(1)
    else:
        logger.error('NOT ENOUGH ARGS')
        sys.exit(1)

    try:
        producer.task()
    except Exception as e:
        logger.error(e)
        producer.save()

