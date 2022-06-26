import sys

import pika

consumer_id = sys.argv[1]

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()
channel.exchange_declare(exchange="messages", exchange_type="direct")

result = channel.queue_declare(queue=f"consumer_{consumer_id}", durable=True)
queue_name = result.method.queue
channel.basic_qos(prefetch_count=1)

channel.queue_bind(
    exchange="messages", queue=queue_name, routing_key=f"consumer_{consumer_id}"
)

print("waiting for messages")


def callback(chn, method, _properties, body):
    if body == b"":
        chn.basic_ack(delivery_tag=method.delivery_tag)
        chn.stop_consuming()
    # time.sleep(0.001)
    # print(" [x] %r:%r" % (method.routing_key, body))
    chn.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

channel.start_consuming()
