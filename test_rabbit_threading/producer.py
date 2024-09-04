import pika, time, signal, sys
import config
from threading import Event, Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

def signal_handler(signum, frame):
    producer.shutdown.set()
    
def signal_init():
    '''Binding signal_handler to SIGINT'''
    signal.signal(signal.SIGINT, signal_handler)
    
class Producer:
    def __init__(self):
        self.shutdown = Event()
        self.lock = Lock()
        
        print("START PRODUCER")
        pass
    
    
    
    def publish(self, id):
        with self.lock:
            print(f"THREAD {id} STARTED")
            cfg_rabbit = config.cfg_rabbit
            credentials = pika.PlainCredentials(cfg_rabbit["user"], cfg_rabbit["password"])
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=cfg_rabbit["host"], virtual_host=cfg_rabbit["virtual_host"], port=cfg_rabbit["port"], credentials=credentials)
            )
            self.channel = self.connection.channel()
            self.channel.exchange_declare('threads', exchange_type='topic', durable=True)
        
        i = 0
        while not self.shutdown.is_set():
            
            self.channel.basic_publish(exchange='threads', routing_key='info.status',
                                            body=f"Thread {id}: {i}",
                                            properties=pika.BasicProperties(
                                            delivery_mode=2))
                
            time.sleep(1)
            i += 1
        
            
        pass
    
    def task(self):
        with ThreadPoolExecutor() as ex:
           futures = [ex.submit(self.publish, i) for i in [0, 1]]
        
           for future in as_completed(futures):
               rez = future.result()
               pass
           
        self.connection.close()
        print("STOP PRODUCER")
        sys.exit(0)
        
if __name__ == "__main__":
    signal_init()
    global producer
    producer = Producer()
    producer.task()