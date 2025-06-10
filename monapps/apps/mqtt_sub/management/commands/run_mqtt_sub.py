import signal
import os
import json

from django.core.management.base import BaseCommand

import paho.mqtt.client as mqtt
from apps.mqtt_sub.put_raw_data_in_db import put_raw_data_in_db
from utils.ts_utils import create_now_ts_ms
from services.alarm_log import add_to_alarm_log


def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        add_to_alarm_log("INFO", "Connected to the broker", create_now_ts_ms(), instance="MQTT Sub")
    sub_topic = os.getenv("MQTT_SUB_TOPIC")
    if sub_topic is None:
        sub_topic = "rawdata/#"  # backup
    client.subscribe(sub_topic, qos=0)


def on_subscribe(client, userdata, mid, reason_code_list, properties):
    add_to_alarm_log("INFO", "Subscribed", create_now_ts_ms(), instance="MQTT Sub")


def on_message(client, userdata, msg):
    # a message topic shoul dlook like:
    # 1. "rawdata/<location>/<sublocation>" - then the payload can have data from several devices and look like
    # {
    #     "dev_ui1":
    #         {"1234567890123": {"e": {...}, "w": {...}, "i": {...}, "ds_name1": {...}, "ds_name2": {...}, ...}, ...}
    #     dev_ui2: {...},
    #     ...
    #    }
    # 2. "rawdata/<location>/<sublocation>/<dev_ui>" - then the payload has data from one device and looks like
    #    {"1234567890123": {"e": {...}, "w": {...}, "i": {...}, "ds_name1": {...}, "ds_name2": {...}, ...}, ...}

    try:
        msg_str = str(msg.payload.decode("utf-8"))
        msg_str_cropped = msg_str[0 : min(30, len(msg_str))] + "..."
        add_to_alarm_log(
            "INFO",
            f"A message on the topic '{msg.topic}' received: '{msg_str_cropped}'",
            create_now_ts_ms(),
            instance="MQTT Sub",
        )
        payload = json.loads(msg_str)
        if type(payload) is not dict:  # TODO: provide validation with pydantic
            raise ValueError("Payload is not a dictionary")
        topic_parts = msg.topic.split("/")
        if len(topic_parts) == 3:
            for dev_ui, dev_payload in payload.items():
                if type(dev_payload) is not dict:
                    add_to_alarm_log(
                        "WARNING",
                        f"Incorrect payload for device '{dev_ui}'",
                        create_now_ts_ms(),
                        instance="MQTT Sub"
                    )
                    continue

                put_raw_data_in_db(dev_ui, dev_payload)

        elif len(topic_parts) == 4:
            dev_ui = topic_parts[3]
            put_raw_data_in_db(dev_ui, payload)
        else:
            raise ValueError("Unknown topic format")

    except Exception as e:
        add_to_alarm_log("ERROR",
                         f"Error while processing a message: {e}",
                         create_now_ts_ms(),
                         instance="MQTT Sub")
        return


def on_disconnect(client: mqtt.Client, userdata, flags, reason_code, properties):
    add_to_alarm_log("INFO",
                     "Disconnected from the broker",
                     create_now_ts_ms(),
                     instance="MQTT Sub")


class Command(BaseCommand):

    mqtt_subscriber = None

    def handle(self, *args, **kwargs):
        self.inner_run(**kwargs)

    def inner_run(self, **kwarg):
        Command.mqtt_subscriber = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id="monappsV3", clean_session=True
        )
        Command.mqtt_subscriber.on_connect = on_connect
        Command.mqtt_subscriber.on_subscribe = on_subscribe
        Command.mqtt_subscriber.on_message = on_message
        Command.mqtt_subscriber.on_disconnect = on_disconnect

        Command.mqtt_subscriber.connect("localhost", 1883, 60)

        Command.mqtt_subscriber.loop_forever()


def handler(signum, frame):
    if Command.mqtt_subscriber is not None:
        Command.mqtt_subscriber.disconnect()


signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGTERM, handler)
