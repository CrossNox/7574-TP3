import pika

CONSUMERS = 3
N_MESSAGES = 50_000

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()
channel.queue_declare(queue="messages", durable=True)

for i in range(N_MESSAGES):
    consumer_id = i % CONSUMERS
    channel.basic_publish(
        exchange="",
        routing_key="messages",
        body=f"message {i}",
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        ),
    )

for i in range(CONSUMERS):
    channel.basic_publish(
        exchange="",
        routing_key="messages",
        body="",
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        ),
    )


print("Sent all messages")
connection.close()
