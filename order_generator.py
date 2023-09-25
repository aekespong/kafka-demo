import json
import time
from datetime import datetime
import sys
import random
from kafka import KafkaProducer
from master import Masterdata
from logger import log

docs = """

    ORDER GENERATOR

    Will generate orders on a topic

"""

print(docs)
ORDER_KAFKA_TOPIC = "order"
ORDER_LIMIT = 1
SLEEP_TIME = 0

if len(sys.argv) > 1:
    ORDER_LIMIT = int(sys.argv[1])


config = {"bootstrap_servers": "localhost:29092", "acks": "all"}

producer = KafkaProducer(**config)

print(f"Order generator will generate {ORDER_LIMIT} orders")

for i in range(1, ORDER_LIMIT + 1):
    now = datetime.now().strftime("%H:%M:%S.%f")[:-2]
    food = Masterdata.translate(type="food", key=int(random.random() * 12))
    drink = Masterdata.translate(type="drinks", key=int(random.random() * 8))

    order = {
        "order_id": i,
        "customer_id": int(10 * random.random()) + 1,
        "total_cost": int(i * 5 * random.random()) + 5,
        "items": f"{food} and a {drink}",
        "x_create_date": now,
    }

    producer.send(ORDER_KAFKA_TOPIC, json.dumps(order).encode("utf-8"))
    now = datetime.now().strftime("%H:%M:%S.%f")[:-2]
    log(f"Order Generator: Publishing order nbr {i} on topic [order] - {now}")
    log(obj=order)
    time.sleep(random.random() * SLEEP_TIME)

producer.flush()
