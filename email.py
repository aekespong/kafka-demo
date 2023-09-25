import json
from logger import log
from datetime import datetime
from kafka import KafkaConsumer


ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"


consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC, bootstrap_servers="localhost:29092"
)

emails_sent_so_far = set()
print("Email: Subscribing to", ORDER_CONFIRMED_KAFKA_TOPIC)
while True:
    for message in consumer:
        message = json.loads(message.value.decode())
        customer_email = message["customer_email"]
        now = datetime.now().strftime("%H:%M:%S.%f")[:-2]
        log(
            f"Email: sent to {customer_email}  -  {now}"
        )
        log(message)
        emails_sent_so_far.add(customer_email)
