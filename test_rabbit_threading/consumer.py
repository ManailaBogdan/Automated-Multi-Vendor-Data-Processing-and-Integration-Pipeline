'''Module for processing data from Rabbitmq queue'''
import sys, pika, select, time, signal, json, datetime
import config



def signal_handler(signum, frame):
        '''
        Graceful shutdown on SIGINT
        Args:
            signum: signal number
            frame: program state object
        '''   

        reciver.close()
            
        
           
        
        
def signal_init():
    '''Binding signal_handler to SIGINT'''
    signal.signal(signal.SIGINT, signal_handler)
   
    

class Reciver:
    def __init__(self):
        '''Initialize consumer object'''
       
        
        # start Rabbitmq connection and chanel
        cfg_rabbit = config.cfg_rabbit
        
        credentials = pika.PlainCredentials(cfg_rabbit["user"], cfg_rabbit["password"])
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=cfg_rabbit["host"], virtual_host=cfg_rabbit["virtual_host"], port=cfg_rabbit["port"], credentials=credentials)
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare('threads', exchange_type='topic', durable=True)
        result = self.channel.queue_declare(queue='threads_queue', durable=True)
        self.queue = result.method.queue
       
        self.channel.queue_bind(exchange='threads', queue=self.queue, routing_key='*.status')
        
        print("START CONSUMER")
        


    def callback(self, ch, method, proprietes, body):
       
        print(body)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        
    
        
   
        

    def task(self):
        '''Start processing data from queue'''
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.callback, auto_ack=False)
        self.channel.start_consuming()
            

        
        

    def close(self):
       
        self.channel.stop_consuming()
        self.connection.close()
        print("STOP CONSUMER")
        sys.exit(0)
        
    


if __name__ == "__main__":
    signal_init()
    global reciver
    reciver = Reciver()
    
    reciver.task()
            
       
