import datetime
import json
import random
import time
import paho.mqtt.client as mqtt

from task1.constants import *


def createJSON():
    data = {
        "fin": FIN,
        "zeit": datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S.%f')[:-3],
        "geschwindigkeit": random.randint(0, 200),
        "ort": 1
    }
    return json.dumps(data)


def main():
    client = mqtt.Client("Publisher_inf19060", clean_session=False)
    client.connect(MQTT_BROKER)

    try:
        while True:
            payload = createJSON()
            print(f"Sending message with payload: {payload}")
            client.publish(MQTT_TOPIC, payload, qos=1)

            time.sleep(5)
    except KeyboardInterrupt:
        pass

    print("Closing connections...")
    client.disconnect()
    print("Done!")


if __name__ == '__main__':
    main()
