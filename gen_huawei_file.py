import os, random, string, datetime
import config


class Huawei:

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
        cfg_huawei = config.cfg_huawei
        while True:
            now = datetime.datetime.now()
            now_str = now.strftime("%Y%m%d%H%M%S")

            name = config.huawei_path + now_str + "_" + self.randomword(cfg_huawei["file_name_len"]) + ".csv"
            if not os.path.exists(name):
                row = "src,record_type,imsi,msisdn,start_date,pGWAddress,chargingID,apn,pdp_address,datavolumeuplink,datavolumedownlink,duration,rat_type\n"
                for _ in range(config.file_size):
                    #src
                    row += "huawei,"
                    #record type
                    row += '50,'
                    #imsi
                    row += "226050123456789,"
                    #msisdn
                    row += "40701234567,"
                    #start date
                    row += "2024-04-10 22-00-00,"
                    #pGWAddress
                    row += "8.8.8.8,"
                    #charging id
                    row += "30000,"
                    #apn
                    row += "ims,"
                    #pdp address
                    row += "9.9.9.9,"
                    #datavolumeuplink
                    row += "10000,"
                    #datavolumedownlink
                    row += "20000,"
                    #duration
                    row += "500,"
                    #rat type
                    row += "1\n"

                with open(name, 'w') as file:
                    file.write(row)

                break

    def gen_file(self):


        cfg_huawei = config.cfg_huawei
        ct = config.file_size
        while True:
            now = datetime.datetime.now()
            now_str = now.strftime("%Y%m%d%H%M%S")

            name = config.huawei_path + now_str + "_" + self.randomword(cfg_huawei["file_name_len"]) + ".csv"
            if not os.path.exists(name):
                row = "src,record_type,imsi,msisdn,start_date,pGWAddress,chargingID,apn,pdp_address,datavolumeuplink,datavolumedownlink,duration,rat_type\n"
                for i in range(config.file_size):
                    #src
                    row += "huawei,"
                    #record type
                    if i == 200 and random.randint(0, 100) > 50:
                        row += '200,'
                        ct -= 1
                    else:
                        row += str(random.randint(cfg_huawei["low_record_type"], cfg_huawei["high_record_type"])) + ","
                    #imsi
                    row += cfg_huawei["imsi_start"] + self.randomnumber(cfg_huawei["imsi_len"] - len(cfg_huawei["imsi_start"])) + ","
                    #msisdn
                    row += cfg_huawei["msisdn_start"] + self.randomnumber(cfg_huawei["msisdn_len"] - len(cfg_huawei["msisdn_start"])) + ","
                    #start date
                    date = self.randomdate(now)
                    row += date.strftime(cfg_huawei["date_format"]) + ","
                    #pGWAddress
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + "."
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + "."
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + "."
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + ","
                    #charging id
                    row += str(random.randint(cfg_huawei["low_charg_id"], cfg_huawei["high_charg_id"])) + ","
                    #apn
                    row += random.choice(cfg_huawei["apn"]) + ","
                    #pdp address
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + "."
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + "."
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + "."
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + ","
                    #datavolumeuplink
                    row += str(random.randint(cfg_huawei["low_up_link"], cfg_huawei["high_up_link"])) + ","
                    #datavolumedownlink
                    row += str(random.randint(cfg_huawei["low_down_link"], cfg_huawei["high_down_link"])) + ","
                    #duration
                    row += str(random.randint(cfg_huawei["low_duration"], cfg_huawei["high_duration"])) + ","
                    #rat type
                    row += str(random.randint(cfg_huawei["low_rat_type"], cfg_huawei["high_rat_type"])) + "\n"

                with open(name, 'w') as file:
                    file.write(row)
                    print(ct)
                break

    def gen_wrong_file(self):
        cfg_huawei = config.cfg_huawei
        date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        name = config.huawei_path + date + "_wrong_" + self.randomword(cfg_huawei["file_name_len"]) + ".csv"
        should_wrong = [True, False]
        done_wrong = False

        if not os.path.exists(name):
            row = "src,record_type,imsi,msisdn,start_date,pGWAddress,chargingID,apn,pdp_address,datavolumeuplink,datavolumedownlink,duration,rat_type\n"
            for _ in range(config.file_size):
                # src
                if random.choice(should_wrong):
                    row += "huawei"
                    done_wrong = True
                else:
                    row += "huawei,"

                # record type
                if random.choice(should_wrong):
                    row += str(random.randint(256, 1000)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_huawei["low_record_type"], cfg_huawei["high_record_type"])) + ","

                # imsi
                if random.choice(should_wrong):
                    row += self.randomnumber(cfg_huawei["imsi_len"]) + ","
                    done_wrong = True
                else:
                    row += cfg_huawei["imsi_start"] + self.randomnumber(cfg_huawei["imsi_len"] - len(cfg_huawei["imsi_start"])) + ","

                # msisdn
                if random.choice(should_wrong):
                    row += self.randomnumber(cfg_huawei["msisdn_len"]) + ","
                    done_wrong = True
                else:
                    row += cfg_huawei["msisdn_start"] + self.randomnumber(cfg_huawei["msisdn_len"] - len(cfg_huawei["msisdn_start"])) + ","

                # start date
                if random.choice(should_wrong):
                    date = self.randomdate("2024-06-01 00-00-00", "2025-04-01 00-00-00")
                    done_wrong = True
                else:
                    date = self.randomdate(cfg_huawei["low_date"], cfg_huawei["high_date"])
                row += date.strftime(cfg_huawei["date_format"]) + ","

                # pGWAddress
                if random.choice(should_wrong):
                    row += str(random.randint(256, 1000)) + "."
                    row += str(random.randint(256, 1000)) + "."
                    row += str(random.randint(256, 1000)) + "."
                    row += str(random.randint(256, 1000)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + "."
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + "."
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + "."
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + ","

                # charging id
                if random.choice(should_wrong):
                    row += str(random.randint(2147483648, 4294967295)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(-2147483648, 2147483647)) + ","

                # apn
                if random.choice(should_wrong):
                    row += self.randomword(cfg_huawei["file_name_len"]) + ","
                    done_wrong = True
                else:
                    row += random.choice(cfg_huawei["apn"]) + ","

                # pdp address
                if random.choice(should_wrong):
                    row += str(random.randint(256, 1000)) + "."
                    row += str(random.randint(256, 1000)) + "."
                    row += str(random.randint(256, 1000)) + "."
                    row += str(random.randint(256, 1000)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + "."
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + "."
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + "."
                    row += str(random.randint(cfg_huawei["low_ip_addr"], cfg_huawei["high_ip_addr"])) + ","

                # datavolumeuplink
                if random.choice(should_wrong):
                    row += str(random.randint(2147483648, 4294967295)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_huawei["low_up_link"], cfg_huawei["high_up_link"])) + ","

                # datavolumedownlink
                if random.choice(should_wrong):
                    row += str(random.randint(2147483648, 4294967295)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_huawei["low_down_link"], cfg_huawei["high_down_link"])) + ","

                # duration
                if random.choice(should_wrong):
                    row += str(random.randint(32768, 65536)) + ","
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_huawei["low_duration"], cfg_huawei["high_duration"])) + ","

                # rat type
                if random.choice(should_wrong):
                    row += str(random.randint(256, 1000)) + "\n"
                    done_wrong = True
                else:
                    row += str(random.randint(cfg_huawei["low_rat_type"], cfg_huawei["high_rat_type"])) + "\n"

                if not done_wrong or random.choice(should_wrong):
                    row = row[:-1]
                    row += ',incorrect_data_example\n'

            with open(name, 'w') as file:
                file.write(row)

huawei = Huawei()
time_start = datetime.datetime.now().timestamp()

huawei.gen_file()

print(datetime.datetime.now().timestamp() - time_start)






