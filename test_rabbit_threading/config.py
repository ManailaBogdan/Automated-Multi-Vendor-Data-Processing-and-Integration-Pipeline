'''RABBIT'''
cfg_rabbit = {
    "user": "admin",
    "password": "LHOdmlLIffII6",
    "host": "rabbit",
    "virtual_host": "/",
    "port": 5672
}

'''VENDOR ALCATEL'''
cfg_alcatel = {
    "field_count": 8,
    "file_name_len": 5,
    "low_id_len": 3,
    "high_id_len": 6,
    "low_t1": 0,
    "high_t1": 255,
    "low_ncu": 0,
    "high_ncu": 255,
    "low_duration": -32768,
    "high_duration": 32767,
    "low_date": "2024-04-01 00-00-00",
    "high_date": "2024-04-30 23-59-59",
    "date_format": "%Y-%m-%d %H-%M-%S",
    "ano_start": "407",
    "ano_len": 11,
    "bno_start": "407",
    "bno_len": 11
}

'''VENDOR NOKIA'''
cfg_nokia = {
    "field_count": 9,
    "file_name_len": 5,
    "low_record_type": 0,
    "high_record_type": 255,
    "low_duration": -32768,
    "high_duration": 32767,
    "low_record_len": 0,
    "high_record_len": 255,
    "low_record_num": -2147483648,
    "high_record_num": 2147483647,
    "low_date": "2024-04-01 00-00-00",
    "high_date": "2024-04-30 23-59-59",
    "date_format": "%Y-%m-%d %H-%M-%S",
    "ano_start": "407",
    "ano_len": 11,
    "call_ref_len": 8,
    "bno_start": "407",
    "bno_len": 11
}

'''VENDOR HUAWEI'''
cfg_huawei = {
    "field_count": 13,
    "file_name_len": 5,
    "low_record_type": 0,
    "high_record_type": 255,
    "low_ip_addr": 0,
    "high_ip_addr": 255,
    "low_charg_id": -2147483648,
    "high_charg_id": 2147483647,
    "imsi_start": "22605",
    "imsi_len": 15,
    "msisdn_start": "407",
    "msisdn_len": 11,
    "low_date": "2024-04-01 00-00-00",
    "high_date": "2024-04-30 23-59-59",
    "date_format": "%Y-%m-%d %H-%M-%S",
    "low_up_link": -2147483648,
    "high_up_link": 2147483647,
    "low_down_link": -2147483648,
    "high_down_link": 2147483647,
    "low_duration": -32768,
    "high_duration": 32767,
    "apn": ["ims", "internet"],
    "low_rat_type": 0,
    "high_rat_type": 255
}

'''ALCATEL FILE PATH AND PREFIX'''
alcatel_path = "/app/rand_files/cdr_alcatel_"

'''NOKIA FILE PATH AND PREFIX'''
nokia_path = "/app/rand_files/cdr_nokia_"

'''HUAWEI FILE PATH AND PREFIX'''
huawei_path = "/app/rand_files/cdr_huawei_"

'''VENDORS DATA TYPES'''
vendors_data_types = {
    "alcatel": ["STRING", "STRING", "INT", "INT", "DATE", "STRING", "INT", "STRING"],
    "nokia": ["STRING", "INT", "DATE", "INT", "INT", "STRING", "INT", "STRING", "STRING"],
    "huawei": ["STRING", "INT", "STRING", "STRING", "DATE", "STRING", "INT", "STRING", "STRING", "INT", "INT", "INT", "INT"]
}

'''VENDOR FILE SIZE'''
file_size = 4000000

'''PATH TO VENDOR FILES'''
source_path = "/app/rand_files/"

'''PATH TO SUCCESSFULLY SENT VENDOR FILES'''
destination_success_path = "/app/success_files/"

'''PATH TO MALFORMED VENDOR FILES'''
destination_error_path = "/app/error_files/"

'''PATH TO IN PROGRESS FILES'''
destination_work_path = "/app/work_files/"

'''UNFINISHED VENDOR FILES'''
path_processed_files = "/app/unfinished_files/"

'''UNFINISHED VENDOR FILES FOLDER'''
folder_unfinished_files = "unfinished_files"

'''NUMBER OF LINES SENT AT ONCE'''
chunk_size = 100

'''NUMBER OF THREADS PER PRODUCER'''
thread_number = 3