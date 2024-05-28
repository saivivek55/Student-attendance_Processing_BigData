from pulsar import Client
pulsar_service_url = 'pulsar://localhost:6650'
client = Client(pulsar_service_url)

# Subscribe to the topic
consumer = client.subscribe('persistent://public/default/attendance', subscription_name='attendance_subscribe')
print("Subscribed to the topic. Waiting for messages...")

try:
    while True:
        msg = consumer.receive()
        try:
            print(f"Received message: '{msg.data().decode('utf-8')}'")
            consumer.acknowledge(msg)
        except Exception as e:
            print(f"Failed to process message: {e}")
            # Negative acknowledgment in case of processing failure
            consumer.negative_acknowledge(msg)
finally:
    consumer.close()
    client.close()