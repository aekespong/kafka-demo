import json
import sys
from kafka import KafkaConsumer, KafkaProducer
from icecream import ic

# config = {"bootstrap_servers": "localhost:29092", "auto_offset_reset": "earliest"}
config = {"bootstrap_servers": "localhost:29092"}
LOGGER_TOPIC = "logger"
DEBUG = True
consumer = KafkaConsumer(LOGGER_TOPIC, **config)
config = {"bootstrap_servers": "localhost:29092", "acks": "all"}
producer = KafkaProducer(**config)


def log(obj, par1: str = "", par2: str = ""):
    if type(obj) == str:
        print(obj, par1, par2)
        print_msg = {
            "msg": obj,
        }
        producer.send(LOGGER_TOPIC, json.dumps(print_msg).encode("utf-8"))
    elif obj is not None:
        log_message = {
            "object": obj,
        }
        producer.send(LOGGER_TOPIC, json.dumps(log_message).encode("utf-8"))
        ic(obj)


def listener():
    print("---------------- Start listening on [logger] --------------------")
    count = 0
    try:
        while True:
            for obj in consumer:
                values = json.loads(obj.value.decode("utf8").replace("'", '"'))
                
                key = next(iter(values))
                if key == "msg" and "Generator" in values[key]:
                    print("")
                if DEBUG or key == "msg":
                    print(json.dumps(values[key], indent=2, sort_keys=True))
                count += 1
    except KeyboardInterrupt:
        print("")
        print(count, "messages logged")
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        DEBUG = False
    listener()
