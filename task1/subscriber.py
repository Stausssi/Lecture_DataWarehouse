import json
from json import JSONDecodeError

import paho.mqtt.client as mqtt
import psycopg2

from task1.constants import *

postgres = psycopg2.connect("dbname='postgres' user='postgres' password='Simon' host='localhost' port='5432'")
postgres.autocommit = True
cursor = postgres.cursor()

read_all_messages = True


def on_message(_, __, message: mqtt.MQTTMessage):
    data = message.payload.decode("utf-8")

    try:
        data = json.loads(data)
        if isinstance(data, dict):
            try:
                if data["fin"] == FIN or read_all_messages:
                    print(f"Received Message: {data}")
                    strData = str(data).replace("'", "\"")
                    try:
                        cursor.execute(f"insert into staging.messung (payload, erstellt_am) values ('{strData}', '{data['zeit']}');")
                        print("Inserted!")
                    except Exception:
                        print("Wrong data format!")
            except KeyError as e:
                print(f"dict contains no FIN: {e}")
    except JSONDecodeError:
        pass
    except Exception as e:
        print(f"Unhandled exception: {e}")


def main():
    client = mqtt.Client("Subscriber_inf19060", clean_session=False)
    client.on_message = on_message
    client.connect(MQTT_BROKER)
    client.subscribe(MQTT_TOPIC, qos=1)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        pass

    print("Closing connections...")
    client.disconnect()
    cursor.close()
    postgres.close()
    print("Done!")


if __name__ == '__main__':
    main()
