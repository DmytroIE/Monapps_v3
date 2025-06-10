import os
import paho.mqtt.client as mqtt

from utils.ts_utils import create_now_ts_ms
from services.alarm_log import add_to_alarm_log


def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        add_to_alarm_log("INFO",
                         "Connected to the broker",
                         create_now_ts_ms(),
                         instance=client._client_id.decode("utf-8"))


def on_disconnect(client: mqtt.Client, userdata, flags, reason_code, properties):
    add_to_alarm_log("INFO",
                     "Disconnected from the broker",
                     create_now_ts_ms(),
                     instance=client._client_id.decode("utf-8"))


proc_name = os.environ.get("MONAPP_PROC_NAME")
publisher_id = None
if proc_name is not None:
    publisher_id = f"MQTT Pub {proc_name}"
    if len(publisher_id) > 22:
        publisher_id = publisher_id[:23]
mqtt_publisher = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=publisher_id, clean_session=True)
mqtt_publisher.on_connect = on_connect
mqtt_publisher.on_disconnect = on_disconnect
mqtt_publisher.connect("localhost", 1883, 60)
mqtt_publisher.loop_start()
