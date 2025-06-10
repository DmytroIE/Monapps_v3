from django.db import models

from apps.devices.models import Device
from apps.datatypes.models import DataType
from common.abstract_classes import PublishingOnSaveModel
from common.constants import VariableTypes, HealthGrades
from utils.ts_utils import create_now_ts_ms
from utils.db_field_utils import create_alarms_field_default


class Datastream(PublishingOnSaveModel):
    """
    Represents a "data stream" from a sensor that is part of a digital device.
    Has "health" that represents how regularly data is coming.
    """

    class Meta:
        db_table = "datastreams"
        constraints = [models.UniqueConstraint(fields=["name", "parent_id"], name="unique_name_device")]

    published_fields = set(["health", "last_reading_ts", "is_enabled"])

    name = models.CharField(max_length=200)  # TODO: should be unique within one device, create a validator

    data_type = models.ForeignKey(DataType, on_delete=models.PROTECT)
    is_totalizer = models.BooleanField(default=False)  # only for 'agg_type' = SUM
    is_rbe = models.BooleanField(default=False)  # report by exception, if t_update is null then it 99% will be True

    parent = models.ForeignKey(
        Device, on_delete=models.CASCADE, related_name="datastreams", related_query_name="datastream"
    )

    t_update = models.BigIntegerField(default=None, blank=True, null=True)  # null for non-periodic datastreams
    t_change = models.BigIntegerField(
        default=None, blank=True, null=True
    )  # not null for CONT + AVG (and maybe for totalizers)

    # a datastream can be deactivated, if all are deactivated, then the parent device is also deactivated
    is_enabled = models.BooleanField(default=True)
    alarms = models.JSONField(default=create_alarms_field_default)

    health = models.IntegerField(default=HealthGrades.UNDEFINED, choices=HealthGrades.choices)  # aggregated health
    # msg_health is health derived from errors/warnings
    msg_health = models.IntegerField(default=HealthGrades.UNDEFINED, choices=HealthGrades.choices)
    # will remain Undefined for non-periodic ds
    nd_health = models.IntegerField(default=HealthGrades.UNDEFINED, choices=HealthGrades.choices)
    t_nd_health_error = models.BigIntegerField(default=300000)
    health_next_eval_ts = models.BigIntegerField(default=0)  # 0 will initiate health eval right after a device creation

    max_rate_of_change = models.FloatField(default=1.0)  # TODO: units per second, can't be <= 0, create a validator
    max_plausible_value = models.FloatField(default=1000000.0)  # TODO: should be > min_plausible_value
    min_plausible_value = models.FloatField(default=-1000000.0)  # TODO: should be < max_plausible_value

    ts_to_start_with = models.BigIntegerField(default=0)  # can be even bigger than 'last_reading_ts'
    last_reading_ts = models.BigIntegerField(default=None, null=True, blank=True)  # only valid reading

    created_ts = models.BigIntegerField(editable=False)

    @property
    def is_value_interger(self) -> bool:
        return self.data_type.var_type != VariableTypes.CONTINUOUS

    def __str__(self):
        return f"Datastream {self.pk} {self.name}"

    __is_enabled = None

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__is_enabled = self.is_enabled

    def save(self, **kwargs):
        if not self.pk:
            # https://stackoverflow.com/questions/1737017/django-auto-now-and-auto-now-add
            self.created_ts = create_now_ts_ms()
        if self.is_enabled is not None and self.__is_enabled != self.is_enabled:
            self.health = HealthGrades.UNDEFINED
            self.msg_health = HealthGrades.UNDEFINED
            self.nd_health = HealthGrades.UNDEFINED
            if "update_fields" in kwargs:
                kwargs["update_fields"] = set([*kwargs["update_fields"], "health", "msg_health", "nd_health"])

        super().save(**kwargs)
        self.__is_enabled = self.is_enabled
