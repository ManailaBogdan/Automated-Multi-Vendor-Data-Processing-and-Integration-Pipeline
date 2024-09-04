import pika

#-----M O N G O-----#

'''VENDORS MONGODB DATABASE'''
mongo_config = {
    'host': 'mongodb://172.17.0.4:27017/'
}

'''MONGO DATABASE NAME'''
mongo_db_name = 'vendors'

#-----M Y S Q L-----#

'''VENDORS MYSQL DATABASE'''
sql_config = {
    'host': '172.17.0.1',
    'user': 'root',
    'password': 'secret',
    'database': 'vendors_db'
}


#-----R A B B I T-----#

'''RABBIT CREDENTIALS'''
rabbit_credentials = {
    "username": "admin",
    "password": "LHOdmlLIffII6",
}

'''RABBIT CONNECTION CREDENTIALS'''
rabbit_connection = {
    "host": "rabbit",
    "virtual_host": "/",
    "port": 5672,
    "credentials": pika.PlainCredentials(**rabbit_credentials)
}

'''RABBIT EXCHANGE FIELDS'''
rabbit_exchange = {
    "exchange": 'vendors_ex',
    "exchange_type": 'topic',
    "durable": True
}

#-----A L C A T E L-----#

'''VENDOR ALCATEL MYSQL QUEUE'''
alcatel_sql_queue = {
    'queue': 'alcatel_sql_queue',
    'durable': True
}

'''ALCATEL SQL BIND'''
alcatel_sql_bind = {
    'exchange': 'vendors_ex',
    'queue': 'alcatel_sql_queue',
    'routing_key': 'vendor.alcatel'
}

'''VENDOR ALCATEL MONGO QUEUE'''
alcatel_mongo_queue = {
    'queue': 'alcatel_mongo_queue',
    'durable': True
}

'''ALCATEL MONGO BIND'''
alcatel_mongo_bind = {
    'exchange': 'vendors_ex',
    'queue': 'alcatel_mongo_queue',
    'routing_key': 'vendor.alcatel'
}

'''VENDOR ALCATEL'''
cfg_alcatel = {
    "field_count": 8,
    "file_name_len": 5,
    "low_id_len": 3,
    "high_id_len": 6,
    "low_t1": -128,
    "high_t1": 127,
    "low_ncu": -128,
    "high_ncu": 127,
    "low_duration": -32768,
    "high_duration": 32767,
    "low_date": "2020-04-01 00-00-00",
    "high_date": "2028-04-30 23-59-59",
    "date_format": "%Y-%m-%d %H-%M-%S",
    "ano_start": "407",
    "ano_len": 11,
    "bno_start": "407",
    "bno_len": 11
}

''''FIELDS TYPES FOR VENDOR ALCATEL'''
fields_types_alcatel = [str, str, int, int, str, str, int, str]

'''START DATE POSITION ALCATEL'''
alcatel_start_date_pos = 4

'''FIELDS COMBINED FOR ALCATEL MD5 HASH'''
md5_alcatel = ['idapp', 'start_date', 'ano']

'''ALCATEL FILE PATH AND PREFIX'''
alcatel_path = "/app/rand_files/cdr_alcatel_"

'''SQL TABLE NAME ALCATEL'''
alcatel_table = 'alcatel'

'''SQL TABLE FIELDS ALCATEL'''
alcatel_table_fields = """id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP,
                    src VARCHAR(7) DEFAULT 'alcatel',
                    idapp VARCHAR(6),
                    t1 TINYINT,
                    ncu TINYINT,
                    start_date TIMESTAMP,
                    ano VARCHAR(11),
                    duration SMALLINT,
                    bno VARCHAR(11),
                    md5 CHAR(32) UNIQUE"""

'''MONGO COLLECTION NAME ALCATEL'''
alcatel_collection = 'alcatel'



#-----N O K I A-----#

'''VENDOR NOKIA SQL QUEUE'''
nokia_sql_queue = {
    'queue': 'nokia_sql_queue',
    'durable': True
}

'''NOKIA SQL BIND'''
nokia_sql_bind = {
    'exchange': 'vendors_ex',
    'queue': 'nokia_sql_queue',
    'routing_key': 'vendor.nokia'
}

'''VENDOR NOKIA MONGO QUEUE'''
nokia_mongo_queue = {
    'queue': 'nokia_mongo_queue',
    'durable': True
}

'''NOKIA MONGO BIND'''
nokia_mongo_bind = {
    'exchange': 'vendors_ex',
    'queue': 'nokia_mongo_queue',
    'routing_key': 'vendor.nokia'
}

'''VENDOR NOKIA'''
cfg_nokia = {
    "field_count": 9,
    "file_name_len": 5,
    "low_record_type": -128,
    "high_record_type": 127,
    "low_duration": -32768,
    "high_duration": 32767,
    "low_record_len": -128,
    "high_record_len": 127,
    "low_record_num": -2147483648,
    "high_record_num": 2147483647,
    "low_date": "2020-04-01 00-00-00",
    "high_date": "2028-04-30 23-59-59",
    "date_format": "%Y-%m-%d %H-%M-%S",
    "ano_start": "407",
    "ano_len": 11,
    "call_ref_len": 8,
    "bno_start": "407",
    "bno_len": 11
}

''''FIELDS TYPES FOR VENDOR NOKIA'''
fields_types_nokia = [str, int, str, int, int, str, int, str, str]

'''START DATE POSITION NOKIA'''
nokia_start_date_pos = 2

'''FIELDS COMBINED FOR NOKIA MD5 HASH'''
md5_nokia = ['start_date', 'ano', 'call_reference', 'bno']

'''NOKIA FILE PATH AND PREFIX'''
nokia_path = "/app/rand_files/cdr_nokia_"

'''SQL TABLE NAME NOKIA'''
nokia_table = 'nokia'

'''SQL TABLE FIELDS NOKIA'''
nokia_table_fields = """id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP,
                    src VARCHAR(5) DEFAULT 'nokia',
                    record_type TINYINT,
                    start_date TIMESTAMP,
                    duration SMALLINT,
                    record_length TINYINT,
                    ano VARCHAR(11),
                    record_number INT,
                    call_reference VARCHAR(8),
                    bno VARCHAR(11),
                    md5 CHAR(32) UNIQUE"""

'''MONGO COLLECTION NAME NOKIA'''
nokia_collection = 'nokia'



#-----H U A W E I-----#

'''VENDOR HUAWEI SQL QUEUE'''
huawei_sql_queue = {
    'queue': 'huawei_sql_queue',
    'durable': True
}

'''HUAWEI SQL BIND'''
huawei_sql_bind = {
    'exchange': 'vendors_ex',
    'queue': 'huawei_sql_queue',
    'routing_key': 'vendor.huawei'
}

'''VENDOR HUAWEI MONGO QUEUE'''
huawei_mongo_queue = {
    'queue': 'huawei_mongo_queue',
    'durable': True
}

'''HUAWEI MONGO BIND'''
huawei_mongo_bind = {
    'exchange': 'vendors_ex',
    'queue': 'huawei_mongo_queue',
    'routing_key': 'vendor.huawei'
}

'''VENDOR HUAWEI'''
cfg_huawei = {
    "field_count": 13,
    "file_name_len": 5,
    "low_record_type": -128,
    "high_record_type": 127,
    "low_ip_addr": 0,
    "high_ip_addr": 255,
    "low_charg_id": -2147483648,
    "high_charg_id": 2147483647,
    "imsi_start": "22605",
    "imsi_len": 15,
    "msisdn_start": "407",
    "msisdn_len": 11,
    "low_date": "2020-04-01 00-00-00",
    "high_date": "2028-04-30 23-59-59",
    "date_format": "%Y-%m-%d %H-%M-%S",
    "low_up_link": -2147483648,
    "high_up_link": 2147483647,
    "low_down_link": -2147483648,
    "high_down_link": 2147483647,
    "low_duration": -32768,
    "high_duration": 32767,
    "apn": ["ims", "internet"],
    "low_rat_type": -128,
    "high_rat_type": 127
}

''''FIELDS TYPES FOR VENDOR HUAWEI'''
fields_types_huawei = [str, int, str, str, str, str, int, str, str, int, int, int, int]

'''START DATE POSITION HUAWEI'''
huawei_start_date_pos = 4

'''FIELDS COMBINED FOR HUAWEI MD5 HASH'''
md5_huawei = ['imsi', 'start_date', 'pGWAddress', 'pdp_address']

'''HUAWEI FILE PATH AND PREFIX'''
huawei_path = "/app/rand_files/cdr_huawei_"

'''SQL TABLE NAME HUAWEI'''
huawei_table = 'huawei'

'''SQL TABLE FILEDS HUAWEI'''
huawei_table_fields = """id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP,
                    src VARCHAR(6) DEFAULT 'huawei',
                    record_type TINYINT,
                    imsi VARCHAR(15),
                    msisdn VARCHAR(11),
                    start_date TIMESTAMP,
                    pGWAddress VARCHAR(15),
                    chargingID INT,
                    apn ENUM('ims', 'internet'),
                    pdp_address VARCHAR(15),
                    datavolumeuplink INT,
                    datavolumedownlink INT,
                    duration SMALLINT,
                    rat_type TINYINT,
                    md5 CHAR(32) UNIQUE"""

'''MONGO COLLECTION NAME HUAWEI'''
huawei_collection = 'huawei'



#-----O T H E R-----#

'''FILE GENERATION FREQUENCY'''
file_frequency = 500

'''VENDOR FILE SIZE'''
file_size = 10000

'''PATH TO VENDOR FILES'''
source_path = "/app/rand_files/"

'''PATH TO SUCCESSFULLY SENT VENDOR FILES'''
destination_success_path = "/app/success_files/"

'''PATH TO MALFORMED VENDOR FILES'''
destination_error_path = "/app/error_files/"

'''PATH TO IN PROGRESS FILES'''
destination_work_path = "/app/work_files/"

'''UNFINISHED VENDOR FILES'''
path_unfinished_files = "/app/unfinished_files/"

'''UNFINISHED VENDOR FILES FOLDER'''
folder_unfinished_files = "unfinished_files"

'''NUMBER OF LINES SENT AT ONCE'''
chunk_size = 1000

'''NUMBER OF THREADS PER PRODUCER'''
thread_number = 3

'''CONNECTION RETRIES'''
connection_retries = 10