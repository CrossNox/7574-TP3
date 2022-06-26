import sys

import pika

consumer_id = sys.argv[1]

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

channel.queue_declare(queue="messages", durable=True)
channel.basic_qos(prefetch_count=1)

print("waiting for messages")


def callback(chn, method, _properties, body):
    if body == b"":
        chn.basic_ack(delivery_tag=method.delivery_tag)
        chn.stop_consuming()
    # time.sleep(0.01)
    # print(" [x] %r:%r" % (method.routing_key, body))
    chn.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue="messages", on_message_callback=callback, auto_ack=False)

channel.start_consuming()
