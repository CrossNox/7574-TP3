import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

channel.exchange_declare(exchange="client", exchange_type="direct")

result = channel.queue_declare(
    queue="", durable=True, exclusive=False, auto_delete=False
)
queue_name = result.method.queue

channel.queue_bind(exchange="client", queue=queue_name, routing_key="posts")
channel.queue_bind(exchange="client", queue=queue_name, routing_key="comments")

channel.confirm_delivery()

print("waiting for messages")


def callback(chn, method, _properties, body):
    if body == b"":
        chn.basic_ack(delivery_tag=method.delivery_tag)
        chn.stop_consuming()
    # time.sleep(0.001)
    print(" [x] %r:%r" % (method.routing_key, body))
    chn.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)


channel.start_consuming()
