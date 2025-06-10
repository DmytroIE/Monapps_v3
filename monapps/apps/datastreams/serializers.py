from rest_framework import serializers
from .models import Datastream
from utils.db_field_utils import get_parent_id, get_instance_full_id


class DsSerializer(serializers.ModelSerializer):

    id = serializers.SerializerMethodField()
    parentId = serializers.SerializerMethodField()
    dataTypeName = serializers.SerializerMethodField()
    measUnit = serializers.SerializerMethodField()
    aggType = serializers.SerializerMethodField()
    varType = serializers.SerializerMethodField()

    def get_id(self, instance):
        return get_instance_full_id(instance)

    def get_parentId(self, instance):
        return get_parent_id(instance)

    def get_dataTypeName(self, instance):
        return instance.data_type.name

    def get_measUnit(self, instance):
        return instance.data_type.meas_unit

    def get_aggType(self, instance):
        return instance.data_type.agg_type

    def get_varType(self, instance):
        return instance.data_type.var_type

    isEnabled = serializers.BooleanField(source="is_enabled")
    isTotalizer = serializers.BooleanField(source="is_totalizer")
    isRbe = serializers.BooleanField(source="is_rbe")
    tUpdate = serializers.IntegerField(source="t_update")
    tChange = serializers.IntegerField(source="t_change")
    maxRateOfChange = serializers.FloatField(source="max_rate_of_change")
    maxPlausibleValue = serializers.FloatField(source="max_plausible_value")
    minPlausibleValue = serializers.FloatField(source="min_plausible_value")
    lastReadingTs = serializers.IntegerField(source="last_reading_ts")

    class Meta:
        model = Datastream
        fields = [
            "id",
            "name",
            "isEnabled",
            "isTotalizer",
            "isRbe",
            "alarms",
            "health",
            "tUpdate",
            "tChange",
            "maxRateOfChange",
            "maxPlausibleValue",
            "minPlausibleValue",
            "lastReadingTs",
            "dataTypeName",
            "measUnit",
            "aggType",
            "varType",
            "parentId",
        ]
