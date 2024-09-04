import pika, json

class Reciver:
    def __init__(self):
        with open("config_rabbit.json", 'r') as file:
            config = file.read()

        config = json.loads(config)
        credentials = pika.PlainCredentials(config["user"], config["password"])
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=config["host"], virtual_host=config["virtual_host"], port=config["port"], credentials=credentials)
        )

        self.channel = self.connection.channel()
        self.channel.queue_declare('first_queue')

    def callback(ch, method, proprietes, body):
        print(f"--------- {body}")


    def task(self):
        self.channel.basic_consume(queue='first_queue', on_message_callback=self.callback, auto_ack=True)
        print('Wait')
        self.channel.start_consuming()

r = Reciver()
r.task()