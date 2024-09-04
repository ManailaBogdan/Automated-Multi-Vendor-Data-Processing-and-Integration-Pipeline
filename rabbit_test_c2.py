import sys, pika, select, time, signal
from configs.rabbit_config import USER, PASSWORD, HOST, VIRTUAL_HOST, PORT
from log_init import logger

def signal_handler(signum, frame):
        if signum == signal.SIGINT:
            logger.info("SIGINT")
            
        elif signum == signal.SIGHUP: 
            logger.info("SIGHUP")
            
        reciver.close()
        
        
def signal_init():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)


class Reciver:
    def __init__(self):
        
       
    
        credentials = pika.PlainCredentials(USER, PASSWORD)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=HOST, virtual_host=VIRTUAL_HOST, port=PORT, credentials=credentials)
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare('topic_ex', exchange_type='topic', durable=True)
        result = self.channel.queue_declare(queue='topic_queue', durable=True)
        self.queue = result.method.queue
        print("Queue " + self.queue)
        self.channel.queue_bind(exchange='topic_ex', queue=self.queue, routing_key='*.status')
        
        logger.info("Connection started")
        
        


    def callback(self, ch, method, proprietes, body):
        print(f"--------- {body}")
        time.sleep(2)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"Got {body}")
       

    def task(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.callback, auto_ack=False)
        print('Wait')
        self.channel.start_consuming()

    def close(self):
        try:
            self.channel.stop_consuming()
        except Exception as e:
            logger.error("Stop consuming")
            
        try:
            self.connection.close()
        except Exception as e:
            logger.error("Connection close")
            
        logger.info("Connection closed")
            
        print("Connection closed")
        sys.exit(0)
        
    


if __name__ == "__main__":
    signal_init()
    global reciver
    reciver = Reciver()
    reciver.task()
