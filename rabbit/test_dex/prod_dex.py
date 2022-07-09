import pika

CONSUMERS = 3
N_MESSAGES = 50_000

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()
channel.exchange_declare(exchange="messages", exchange_type="direct")

for i in range(CONSUMERS):
    result = channel.queue_declare(queue=f"consumer_{i}", durable=True)
    queue_name = result.method.queue
    channel.queue_bind(
        exchange="messages", queue=queue_name, routing_key=f"consumer_{i}"
    )


for i in range(N_MESSAGES):
    consumer_id = i % CONSUMERS
    channel.basic_publish(
        exchange="messages",
        routing_key=f"consumer_{consumer_id}",
        body=f"message {i}",
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        ),
    )

for i in range(CONSUMERS):
    channel.basic_publish(
        exchange="messages",
        routing_key=f"consumer_{i}",
        body="",
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        ),
    )


print("Sent all messages")
connection.close()
