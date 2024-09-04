import sys, pika, select, time, signal, json
import config
from log_init import logger
from undone_msg import DONE, STAGE, BODY


def signal_handler(signum, frame):
        if signum == signal.SIGINT:
            reciver.on_SIGINT()
            logger.info("SIGINT")
            
        elif signum == signal.SIGHUP:
            reciver.on_SIGHUP() 
            logger.info("SIGHUP")
            
        reciver.close()
            
        
           
        
        
def signal_init():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)
    

class Reciver:
    def __init__(self):
        cfg_rabbit = config.cfg_rabbit
        self.completion_stage = 0
        self.mode = sys.argv[1]
        
        credentials = pika.PlainCredentials(cfg_rabbit["user"], cfg_rabbit["password"])
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=cfg_rabbit["host"], virtual_host=cfg_rabbit["virtual_host"], port=cfg_rabbit["port"], credentials=credentials)
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare('exchange_2', exchange_type='direct', durable=True)
        result = self.channel.queue_declare(queue='topic_'+self.mode, durable=True)
        self.queue = result.method.queue
        print("Queue " + self.queue)
        self.channel.queue_bind(exchange='exchange_2', queue=self.queue, routing_key=self.mode)
        logger.info("Connection started")
        
        


    def callback(self, ch, method, proprietes, body):
        self.current_method = method
        self.current_body = body
        self.completion_stage = 0
     
        time.sleep(2)
        with open(self.mode.lower() + ".txt", 'a') as file:
            file.write(f"{body}\n")
        logger.info("Added to db1")
        self.completion_stage += 1
        
       
        
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
    
        
    def undone_task(self):
        if not DONE:
            logger.info("Undone task")
            if STAGE <= 0:
                time.sleep(2)
                with open("db1.txt", 'a') as file:
                    file.write(f"{BODY}\n")
                logger.info("Added to db1")
                self.completion_stage += 1
            elif STAGE <= 1:
                time.sleep(2)
                with open("db2.txt", 'a') as file:
                    file.write(f"{BODY}\n")
                logger.info("Added to db1")
                self.completion_stage += 1
        
            with open("undone_msg.py", 'w') as file:
                    file.write("DONE = True\n")
                    file.write(f"STAGE = 0\n")
                    file.write(f"BODY = \"\"\n")
                
        

    def task(self):
        self.undone_task()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.callback, auto_ack=False)
        print('Wait')
        try: 
            self.channel.start_consuming()
        except Exception as e:
            logger.error("Start consuming")
            
    def on_SIGINT(self):
        tag = self.current_method.delivery_tag
         
        if self.completion_stage == 0:
            self.channel.basic_nack(delivery_tag=tag, requeue=True)
        elif self.completion_stage == 1:
            self.channel.basic_nack(delivery_tag=tag, requeue=False)
        else:
            self.channel.basic_ack(delivery_tag=tag)
           
        
    def on_SIGHUP(self):
        tag = self.current_method.delivery_tag
        if self.completion_stage == 0:
            self.channel.basic_nack(delivery_tag=tag, requeue=True)
        elif self.completion_stage == 1:
            self.channel.basic_nack(delivery_tag=tag, requeue=False)
            with open("undone_msg.py", 'w') as file:
                file.write("DONE = False\n")
                file.write(f"STAGE = {self.completion_stage}\n")
                file.write(f"BODY = {self.current_body}\n")
        else:
            self.channel.basic_ack(delivery_tag=tag)
                
        
            
   

    def close(self):
        
        try:
            self.channel.stop_consuming()
        except Exception as e:
            logger.error("Stop_consuming")
            
        try:
            self.connection.close()
        except Exception as e:
            logger.error("Connection close")
            
        logger.info("Connection closed")
    
        sys.exit(0)
        
    


if __name__ == "__main__":
    signal_init()
    global reciver
    reciver = Reciver()
    reciver.task()
