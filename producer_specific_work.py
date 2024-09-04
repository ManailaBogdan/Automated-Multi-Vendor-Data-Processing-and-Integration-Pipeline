import sys, pika, time, signal, os, json, random
from threading import Event
import config

from log_init import logger


path = "rand_files/"

def signal_handler(signum, frame):
        if signum == signal.SIGINT:
            logger.info("SIGINT")
            
        elif signum == signal.SIGHUP: 
            logger.info("SIGHUP")
           
        sender.shutdown.set()
        
def signal_init():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)

class Sender:
    def __init__(self):
        
        self.shutdown = Event()
        
        
        cfg_rabbit = config.cfg_rabbit
        credentials = pika.PlainCredentials(cfg_rabbit["user"], cfg_rabbit["password"])
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=cfg_rabbit["host"], virtual_host=cfg_rabbit["virtual_host"], port=cfg_rabbit["port"], credentials=credentials)
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare('exchange_2', exchange_type='direct', durable=True)
        logger.info("Connection started")
        
        
    
            
        
    def task(self):
       
            
        files = [f for f in os.listdir(os.path.abspath(path))]
       
        random.shuffle(files)
        
        for file in files:
            msg = ""
            if "alcatel" in file:
                msg = "ALCATEL"
            elif "nokia" in file:
                msg = "NOKIA"
            elif "huawei" in file:
                msg = "HUAWEI"
                
            self.channel.basic_publish(exchange='exchange_2', routing_key=msg, body=msg, properties=pika.BasicProperties(delivery_mode=2))
            time.sleep(1)
        
       
      
        self.close()
        
               
    def str_to_type(self, str, type):
        if type == "STRING":
            return str
        elif type == "INT":
            return int(str)
        elif type == "DATE":
            return str 
       
    def make_dict(self, keys, vals, types):
        d = {}
        for key, val, type in zip(keys, vals, types):
            d[key] = self.str_to_type(val, type)
            
        return d
            
    def close(self):
     
        try:
            self.connection.close()
        except Exception as e:
            logger.error(f"{e}")
                
        logger.info("Connection closed")
       
        sys.exit(0)
            
        
       

if __name__ == "__main__":
    signal_init()
    global sender
    sender = Sender()
    sender.task()