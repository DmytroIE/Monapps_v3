import json
import humps

from django.db import models
from django.conf import settings

from utils.db_field_utils import get_parent_id, get_instance_full_id
from utils.ts_utils import create_dt_from_ts_ms, create_now_ts_ms
from services.alarm_log import add_to_alarm_log
from services.mqtt_publisher import mqtt_publisher


class PublishingOnSaveModel(models.Model):

    class Meta:
        abstract = True

    published_fields = set()
    name = "PublishingOnSaveModel instance"  # backup, if 'name' was forgotten to be defined in a subclass

    # TODO: overload the 'delete' method as well

    def save(self, **kwargs):

        super().save(**kwargs)
        if mqtt_publisher is not None:
            # If 'update_fields' is None or its length > 0, which means that
            # there are some real changes in the saved instance (we assume that we save any model
            # only when some of its fields were changed, so the database is not hit for no reason).
            # Also, it is very important to publish on MQTT only when some fields of the model has changed
            # because the frontend app will also react when new publishing takes place
            update_fields = kwargs.get("update_fields")
            # if the result of 'kwargs.get("update_fields")' is None, it will substitute
            # both '"update_fields" not in kwargs' and 'kwargs["update_fields"] is None'
            if update_fields is None or (len(update_fields) > 0 and self.published_fields.intersection(update_fields)):
                mqtt_pub_dict = self.create_mqtt_pub_dict()

                topic = f"procdata/{settings.INSTANCE_ID}/{self._meta.model_name}/{self.pk}"
                payload_str = json.dumps(mqtt_pub_dict)
                mess_info = mqtt_publisher.publish(topic, payload_str, qos=0, retain=True)
                add_to_alarm_log("INFO",
                                 "Changes published",
                                 create_now_ts_ms(),
                                 instance=self)

    def create_mqtt_pub_dict(self):
        mqtt_pub_dict = {}
        mqtt_pub_dict["id"] = get_instance_full_id(self)
        mqtt_pub_dict["name"] = self.name
        mqtt_pub_dict["parentId"] = get_parent_id(self)

        for field in self.published_fields:
            attr = getattr(self, field, "NO_ATTR")
            if attr == "NO_ATTR":
                print(f"No attribute {field} in {self} to publish")  # TODO: is it to be published to the alarm log?
                continue
            camelized_field = humps.camelize(field)
            mqtt_pub_dict[camelized_field] = attr

        return mqtt_pub_dict


class AnyDsReading(models.Model):
    class Meta:
        abstract = True

    short_name = ""

    pk = models.CompositePrimaryKey("datastream_id", "time")
    time = models.BigIntegerField()
    datastream = models.ForeignKey("datastreams.Datastream", on_delete=models.PROTECT)
    db_value = models.FloatField()

    @property
    def value(self) -> float | int:
        if self.datastream.is_value_interger:
            return round(self.db_value)
        else:
            return self.db_value

    @value.setter
    def value(self, value: float) -> None:
        self.db_value = value

    def __str__(self):
        dt_str = create_dt_from_ts_ms(self.time).strftime("%Y/%m/%d %H:%M:%S")
        return f"{self.short_name} ds:{self.datastream.pk} ts:{dt_str} val: {self.value}"


class AnyNoDataMarker(models.Model):

    class Meta:
        abstract = True

    short_name = ""

    pk = models.CompositePrimaryKey("datastream_id", "time")
    time = models.BigIntegerField()
    datastream = models.ForeignKey("datastreams.Datastream", on_delete=models.PROTECT)

    def __str__(self):
        dt_str = create_dt_from_ts_ms(self.time).strftime("%Y/%m/%d %H:%M:%S")
        return f"{self.short_name} ds:{self.datastream.pk} ts:{dt_str}"
