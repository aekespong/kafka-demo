import json
from logger import log
from icecream import ic
from kafka import KafkaConsumer
from datetime import datetime

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"


consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC, bootstrap_servers="localhost:29092"
)

total_orders_count = 0
total_revenue = 0
print("Analytics: Subscribing to:", ORDER_CONFIRMED_KAFKA_TOPIC)
while True:
    for message in consumer:
        order_confirmed = json.loads(message.value.decode())
        ic(order_confirmed)
        total_cost = float(order_confirmed["total_cost"])
        total_orders_count += 1
        total_revenue += total_cost
        now = datetime.now().strftime("%H:%M:%S.%f")[:-2]
        log(
            f"Analytics: {total_orders_count} orders for a total of {total_revenue:.2f} USD - {now}" # noqa
        )
        
