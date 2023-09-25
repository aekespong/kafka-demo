import json
import sys
from logger import log
from kafka import KafkaConsumer
from kafka import KafkaProducer
from datetime import datetime
from master import Masterdata

docs = """

    ORDER BACKEND

    Will transform incoming ORDERS and publish them on another topic

"""
print(docs)
ORDER_KAFKA_TOPIC = "order"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

# 'auto_offset_reset': 'earliest' för att börja från första position i kön
config = {"bootstrap_servers": "localhost:29092"}

consumer = KafkaConsumer(ORDER_KAFKA_TOPIC, **config)
producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("ORDER BACKEND: Subscribing to:", ORDER_KAFKA_TOPIC)


def consume_message():
    order_nbr = 0
    for message in consumer:
        now = datetime.now().strftime("%H:%M:%S.%f")[:-2]
        order = json.loads(message.value.decode())
        order_id = order["order_id"]
        print("")
        log(f"Transformer: Recieving order {order_id} from [order]    -  {now} ")
        log(order)

        customer_id = order["customer_id"]

        total_cost = order["total_cost"]
        items = order["items"]
        order_confirmed = {
            "order_nbr_confirmed": order_nbr,
            "order_id": order["order_id"],
            "customer": Masterdata.translate("customer", customer_id),
            "customer_email": f'{Masterdata.translate("customer", customer_id).lower()}@gmail.com',
            "mail body": f"Your order of {items} is confirmed!",
            "total_cost": f"{total_cost:.2f}",
            "x_sent_date": now,
        }

        log(
            f"Transformer: Publishing order {order_id} to [order_confirmed]   -  {now} "
        )
        log(order_confirmed)
        order_nbr += 1
        producer.send(
            ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(order_confirmed).encode("utf-8")
        )


if __name__ == "__main__":
    try:
        while True:
            consume_message()
    except KeyboardInterrupt:
        print(("\nNo more transforms!"))
        sys.exit(0)