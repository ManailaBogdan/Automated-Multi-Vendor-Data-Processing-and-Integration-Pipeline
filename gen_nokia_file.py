import os,  random, string, datetime
import config
from concurrent.futures import ThreadPoolExecutor, as_completed
class Nokia:

    def __init__(self):
        pass

    def randomword(self, length):
        letters = string.ascii_letters
        return ''.join(random.choice(letters) for i in range(length))

    def randomnumber(self, length):
        return ''.join(random.choice(string.digits) for i in range(length))



    def randomdate(self, start):
        random_second = random.randint(0, 300)
        return start - datetime.timedelta(seconds=random_second)

    def gen_standard(self):
        cfg_nokia = config.cfg_nokia
        while True:
            date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            name = config.nokia_path + date + "_" + self.randomword(cfg_nokia["file_name_len"]) + ".csv"
            if not os.path.exists(name):
                row = "src,record_type,start_date,duration,record_length,ano,record_number,call_reference,bno\n"
                for _ in range(config.file_size):
                    #src
                    row += "nokia,"
                    #record type
                    row += "120,"
                    #strat date
                    row += "2024-04-10 22-00-00,"
                    #duration
                    row += "5,"
                    #record length
                    row += "75,"
                    #ano
                    row += "40712345678,"
                    #record number
                    row += "100005,"
                    #call reference
                    row += "abcdefgh,"
                    #bno
                    row += "40701234567\n"

                with open(name, 'w') as file:
                    file.write(row)

                break

    def gen_file(self):


        cfg_nokia = config.cfg_nokia
        ct = config.file_size
        while True:
            now = datetime.datetime.now()
            now_str = now.strftime("%Y%m%d%H%M%S")

            name = config.nokia_path + now_str + "_" + self.randomword(cfg_nokia["file_name_len"]) + ".csv"
            if not os.path.exists(name):
                row = "src,record_type,start_date,duration,record_length,ano,record_number,call_reference,bno\n"
                for i in range(config.file_size):
                    #src
                    row += "nokia,"
                    #record type
                    if i == 200 and random.randint(0, 100) > 50:
                        row += '200,'
                        ct -= 1
                    else:
                        row += str(random.randint(cfg_nokia["low_record_type"], cfg_nokia["high_record_type"])) + ","
                    #strat date
                    date = self.randomdate(now)
                    row += date.strftime(config.cfg_nokia["date_format"]) + ","
                    #duration
                    row += str(random.randint(cfg_nokia["low_duration"], cfg_nokia["high_duration"])) + ","
                    #record length
                    row += str(random.randint(cfg_nokia["low_record_len"], cfg_nokia["high_record_len"])) + ","
                    #ano
                    row += cfg_nokia["ano_start"] + self.randomnumber(cfg_nokia["ano_len"] - len(cfg_nokia["ano_start"])) + ","
                    #record number
                    row += str(random.randint(cfg_nokia["low_record_num"], cfg_nokia["high_record_num"])) + ","
                    #call reference
                    row += self.randomword(cfg_nokia["call_ref_len"]) + ","
                    #bno
                    row += cfg_nokia["bno_start"] + self.randomnumber(cfg_nokia["bno_len"] - len(cfg_nokia["bno_start"])) + "\n"

                with open(name, 'w') as file:
                    file.write(row)

                print(ct)
                break

    def gen_wrong_file(self):
        cfg_nokia = config.cfg_nokia
        date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        name = config.nokia_path + date + "_wrong_" + self.randomword(cfg_nokia["file_name_len"]) + ".csv"
        should_wrong = [True, False]
        done_wrong = False

        if not os.path.exists(name):
            row = "src,record_type,start_date,duration,record_length,ano,record_number,call_reference,bno\n"
            for _ in range(config.file_size):
                # src
                if random.choice(should_wrong):
                    row += "nokia"
                    done_wrong = True
                else:
                    row += "nokia,"

                # record type
                if random.choice(should_wrong):
                    row += str(random.randint(256, 1000)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_nokia["low_record_type"], cfg_nokia["high_record_type"])) + ","

                # start date
                if random.choice(should_wrong):
                    date = self.randomdate("2024-06-01 00-00-00", "2025-04-01 00-00-00")
                    done_wrong = True
                else:
                    date = self.randomdate(cfg_nokia["low_date"], cfg_nokia["high_date"])
                row += date.strftime(config.cfg_nokia["date_format"]) + ","

                # duration
                if random.choice(should_wrong):
                    row += str(random.randint(32768, 65536)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_nokia["low_duration"], cfg_nokia["high_duration"])) + ","

                # record length
                if random.choice(should_wrong):
                    row += str(random.randint(256, 1000)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_nokia["low_record_len"], cfg_nokia["high_record_len"])) + ","

                # ano
                if random.choice(should_wrong):
                    row += "1234t" + self.randomnumber(cfg_nokia["ano_len"] - len(cfg_nokia["ano_start"])) + ","
                    done_wrong = True
                else:
                    row += cfg_nokia["ano_start"] + self.randomnumber(cfg_nokia["ano_len"] - len(cfg_nokia["ano_start"])) + ","

                # record number
                if random.choice(should_wrong):
                    row += str(random.randint(2147483648, 4294967295)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_nokia["low_record_num"], cfg_nokia["high_record_num"])) + ","

                # call reference
                if random.choice(should_wrong):
                    row += self.randomword(2 * cfg_nokia["call_ref_len"]) + ","
                    done_wrong = True
                else:
                    row += self.randomword(cfg_nokia["call_ref_len"]) + ","

                # bno
                if random.choice(should_wrong):
                    row += cfg_nokia["bno_start"] + self.randomnumber(20 - len(cfg_nokia["bno_start"])) + "\n"
                    done_wrong = True
                else:
                    row += cfg_nokia["bno_start"] + self.randomnumber(cfg_nokia["bno_len"] - len(cfg_nokia["bno_start"])) + "\n"

                if not done_wrong or random.choice(should_wrong):
                    row = row[:-1]
                    row += ',incorrect_data_example\n'

            with open(name, 'w') as file:
                file.write(row)


nokia = Nokia()
time_start = datetime.datetime.now().timestamp()
# with ThreadPoolExecutor() as ex:
#     futures = [ex.submit(nokia.gen_file())
#             for i in range(10)]
#     for future in as_completed(futures):
#         try:
#             _ = future.result()
#         except:
#             pass
nokia.gen_file()
print(datetime.datetime.now().timestamp() - time_start)




