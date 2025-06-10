from django.db import models
from django_celery_beat.models import PeriodicTask, IntervalSchedule

from apps.assets.models import Asset
from common.abstract_classes import PublishingOnSaveModel
from common.constants import (
    StatusTypes,
    CurrStateTypes,
    StatusUse,
    CurrStateUse,
    HealthGrades,
    AppPurps,
    DEFAULT_T_RESAMPLE,
    DEFAULT_T_STATUS_STALE,
    DEFAULT_T_CURR_STATE_STALE,
    DEFAULT_T_APP_HEALTH_ERROR,
    STATUS_FIELD_NAME,
    CURR_STATE_FIELD_NAME,
)
from utils.ts_utils import create_now_ts_ms
from utils.db_field_utils import create_alarms_field_default


class AppType(models.Model):

    class Meta:
        db_table = "app_types"

    name = models.CharField(max_length=200, unique=True)
    description = models.TextField(max_length=1000, blank=True)
    main_purp = models.CharField(default=AppPurps.NONE, choices=AppPurps.choices)

    # looks like {name1: {datatype: datatype_name1, derived: bool}, name2: ...}, for instance,
    # {"Temp inlet": {"datatype: "Temperature", "derived": False},
    #  "Temp outlet": {"datatype: "Temperature", "derived": False},
    #  "Current state": {"datatype": "State", "derived": True}
    # 'derived' dfs don't have a corresponding datastream
    df_schema = models.JSONField(default=dict, blank=True)

    # F.e. {"Delta": {"type": "number", "minimum": 0}, "Delay, sec": {"type": "integer", "minimum": 0, "maximum": 120}}
    # -> https://json-schema.org/understanding-json-schema/basics
    settings_jsonschema = models.JSONField(default=dict, blank=True)
    func_name = models.CharField(max_length=200)

    @property
    def has_status(self) -> bool:
        return bool(self.df_schema.get(STATUS_FIELD_NAME))

    @property
    def has_curr_state(self) -> bool:
        return bool(self.df_schema.get(CURR_STATE_FIELD_NAME))

    def __str__(self):
        return f"AppType '{self.name}'"


class Application(PublishingOnSaveModel):

    class Meta:
        db_table = "applications"

    published_fields = set(
        [
            "cursor_ts",
            "status",
            "is_status_stale",
            "last_status_update_ts",
            "curr_state",
            "is_curr_state_stale",
            "last_curr_state_update_ts",
            "health",
            "is_enabled",
            "is_catching_up",
        ]
    )

    type = models.ForeignKey(AppType, on_delete=models.PROTECT)
    t_resample = models.BigIntegerField(default=DEFAULT_T_RESAMPLE)

    settings = models.JSONField(default=dict, blank=True)  # application settings in format "{valid from": {settings},}
    state = models.JSONField(default=dict, blank=True)  # for retaining the state between calculations
    alarms = models.JSONField(default=create_alarms_field_default)

    cursor_ts = models.BigIntegerField(default=0)
    is_enabled = models.BooleanField(default=False)

    invoc_interval = models.ForeignKey(
        IntervalSchedule, on_delete=models.PROTECT, related_name="norm_speed_apps", related_query_name="norm_speed_app"
    )
    catch_up_interval = models.ForeignKey(
        IntervalSchedule,
        on_delete=models.PROTECT,
        related_name="catching_up_apps",
        related_query_name="catching_up_app",
    )
    is_catching_up = models.BooleanField(default=False)

    func_version = models.CharField(max_length=200, default="1.0.0")

    status = models.IntegerField(default=None, choices=StatusTypes.choices, blank=True, null=True)
    curr_state = models.IntegerField(default=None, choices=CurrStateTypes.choices, blank=True, null=True)
    last_status_update_ts = models.BigIntegerField(default=None, blank=True, null=True)
    last_curr_state_update_ts = models.BigIntegerField(default=None, blank=True, null=True)
    t_status_eval = models.BigIntegerField(default=DEFAULT_T_RESAMPLE)
    t_curr_state_eval = models.BigIntegerField(default=DEFAULT_T_RESAMPLE)
    status_use = models.IntegerField(default=StatusUse.AS_WARNING, choices=StatusUse.choices, blank=False, null=False)
    curr_state_use = models.IntegerField(
        default=CurrStateUse.AS_WARNING, choices=CurrStateUse.choices, blank=False, null=False
    )

    t_status_stale = models.BigIntegerField(default=DEFAULT_T_STATUS_STALE)
    t_curr_state_stale = models.BigIntegerField(default=DEFAULT_T_CURR_STATE_STALE)
    is_status_stale = models.BooleanField(default=False)
    is_curr_state_stale = models.BooleanField(default=False)

    task = models.OneToOneField(PeriodicTask, on_delete=models.SET_NULL, null=True, blank=True, default=None)

    health = models.IntegerField(default=HealthGrades.UNDEFINED, choices=HealthGrades.choices)
    # should be > 4 * t_change + t_resample if there is a datafeed CONT + AVG + restoration enabled
    t_health_error = models.BigIntegerField(default=DEFAULT_T_APP_HEALTH_ERROR)

    parent = models.ForeignKey(
        Asset,
        on_delete=models.SET_NULL,
        default=None,
        null=True,
        blank=True,
        related_name="applications",
        related_query_name="application",
    )

    created_ts = models.BigIntegerField(editable=False)

    @property
    def name(self):
        return self.type.name

    __is_enabled = None

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__is_enabled = self.is_enabled

    def save(self, **kwargs):
        if not self.pk:
            # https://stackoverflow.com/questions/1737017/django-auto-now-and-auto-now-add
            self.created_ts = create_now_ts_ms()
        if self.__is_enabled is not None and self.__is_enabled != self.is_enabled:
            self.health = HealthGrades.UNDEFINED
            if "update_fields" in kwargs:
                kwargs["update_fields"] = set([*kwargs["update_fields"], "health"])

        super().save(**kwargs)
        self.__is_enabled = self.is_enabled

    def __str__(self):
        return f"Application {self.pk} '{self.type.name}'"

    def get_native_df_qs(self):
        return self.datafeeds.exclude(datastream__isnull=True)

    def get_derived_df_qs(self):
        return self.datafeeds.filter(datastream__isnull=True)
