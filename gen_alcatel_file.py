import os, random, string, datetime
from log_init import logger
import config

class Alcatel:

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
        cfg_alcatel = config.cfg_alcatel
        ct = config.file_size
        while True:
            date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            name = config.alcatel_path + date + "_" + self.randomword(cfg_alcatel["file_name_len"]) + ".csv"
            if not os.path.exists(name):
                row = "src,idapp,t1,ncu,start_date,ano,duration,bno\n"
                for i in range(config.file_size):
                    #src
                    row += "alcatel,"
                    #idapp
                    row += "abc,"
                    #t1
                    row += "127,"
                    #ncu
                    if i == 200 and random.randint(75, 100) > 50:
                        row += "200,"
                        ct -= 1
                    else:
                        row += "100,"
                    #start date
                    if i == 500 and random.randint(0, 49) > 50:
                        row += "2025-04-10 22-00-00,"
                        ct -= 1
                    else:
                        row += "2024-04-10 22-00-00,"
                    #ano
                    row += "40712345678,"
                    #duration
                    row += "1000,"
                    #bno
                    row += "40787654321\n"

                with open(name, 'w') as file:
                    file.write(row)

                print(ct)
                break


    def gen_file(self):
        ct = config.file_size
        cfg_alcatel = config.cfg_alcatel
        while True:
            now = datetime.datetime.now()
            now_str = now.strftime("%Y%m%d%H%M%S")

            name = config.alcatel_path + now_str + "_" + self.randomword(cfg_alcatel["file_name_len"]) + ".csv"
            if not os.path.exists(name):
                row = "src,idapp,t1,ncu,start_date,ano,duration,bno\n"
                for i in range(config.file_size):
                    #src
                    row += "alcatel,"
                    #idapp
                    nr_letters = random.randint(cfg_alcatel["low_id_len"], cfg_alcatel["high_id_len"])
                    row += self.randomword(nr_letters) + ","
                    #t1
                    if i == 200 and random.randint(0, 100) > 50:
                        row += '200,'
                        ct -= 1
                    else:
                        row += str(random.randint(cfg_alcatel["low_t1"], cfg_alcatel["high_t1"])) + ","
                    #ncu
                    row += str(random.randint(cfg_alcatel["low_ncu"], cfg_alcatel["high_ncu"])) + ","
                    #start date
                    date = self.randomdate(now)
                    row += date.strftime(cfg_alcatel["date_format"]) + ","
                    #ano
                    row += cfg_alcatel["ano_start"] + self.randomnumber(cfg_alcatel["ano_len"] - len(cfg_alcatel["ano_start"])) + ","
                    #duration
                    row += str(random.randint(cfg_alcatel["low_duration"], cfg_alcatel["high_duration"])) + ","
                    #bno
                    row += cfg_alcatel["bno_start"] + self.randomnumber(cfg_alcatel["bno_len"] - len(cfg_alcatel["bno_start"])) + "\n"

                with open(name, 'w') as file:
                    file.write(row)

                print(ct)
                break


    def gen_wrong_file(self):
        cfg_alcatel = config.cfg_alcatel
        date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        name = config.alcatel_path + date + "_wrong_" + self.randomword(cfg_alcatel["file_name_len"]) + ".csv"
        should_wrong = [True, False]
        done_wrong = False
        if not os.path.exists(name):
            row = "src,idapp,t1,ncu,start_date,ano,duration,bno\n"
            for _ in range(config.file_size):
                #src
                if random.choice(should_wrong):
                    row += 'alcatel'
                    done_wrong = True
                else:
                    row += "alcatel,"

                #idapp
                if random.choice(should_wrong):
                    nr_letters = random.randint(0, 20)
                    row += self.randomword(nr_letters) + ","
                    done_wrong = True
                else:
                    nr_letters = random.randint(cfg_alcatel["low_id_len"], cfg_alcatel["high_id_len"])
                    row += self.randomword(nr_letters) + ","
                #t1
                if random.choice(should_wrong):
                    row += str(random.randint(255, 75674)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_alcatel["low_t1"], cfg_alcatel["high_t1"])) + ","

                #ncu
                if random.choice(should_wrong):
                    row += str(random.randint(0, 255)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_alcatel["low_ncu"], cfg_alcatel["high_ncu"])) + ","
                #start date
                if random.choice(should_wrong):
                    date = self.randomdate("2024-06-01 00-00-00", "2025-04-01 00-00-00")
                    done_wrong = True
                else:
                    date = self.randomdate(cfg_alcatel["low_date"], cfg_alcatel["high_date"])
                row += date.strftime(cfg_alcatel["date_format"]) + ","
                #ano
                if random.choice(should_wrong):
                    row += "8643t69" + self.randomnumber(cfg_alcatel["ano_len"] - len(cfg_alcatel["ano_start"])) + ","
                    done_wrong = True
                else:
                    row += cfg_alcatel["ano_start"] + self.randomnumber(cfg_alcatel["ano_len"] - len(cfg_alcatel["ano_start"])) + ","

                #duration
                if random.choice(should_wrong):
                    row += str(random.randint(77777, 99999)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_alcatel["low_duration"], cfg_alcatel["high_duration"])) + ","
                #bno
                if random.choice(should_wrong):
                    row += cfg_alcatel["bno_start"] + self.randomnumber(20 - len(cfg_alcatel["bno_start"])) + "\n"
                    done_wrong = True
                else:
                    row += cfg_alcatel["bno_start"] + self.randomnumber(cfg_alcatel["bno_len"] - len(cfg_alcatel["bno_start"])) + "\n"

                if not done_wrong or random.choice(should_wrong):
                    row = row[:-1]
                    row += ',incorrect_data_example\n'

            with open(name, 'w') as file:
                file.write(row)




alcatel = Alcatel()
time_start = datetime.datetime.now().timestamp()

alcatel.gen_file()


print(datetime.datetime.now().timestamp() - time_start)







