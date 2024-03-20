from confluent_kafka import Producer

def read_config():
    # reads the client configuration from client.properties
    # and returns it as a key-value map
    config = {}
    with open("./client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config


def main(key, value):
    config = read_config()
    topic = "elexon_topic"

    # creates a new producer instance
    producer = Producer(config)

    # produces a message
    producer.produce(topic, key=key, value=value)
    print(f"Produced message to topic {topic}: key= {key} value = {value}.")

    # send any outstanding or buffered messages to the Kafka broker
    producer.flush()
