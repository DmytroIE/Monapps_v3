from rest_framework import serializers

from .models import Datafeed
from utils.db_field_utils import get_parent_id, get_instance_full_id


class DfSerializer(serializers.ModelSerializer):

    id = serializers.SerializerMethodField()
    parentId = serializers.SerializerMethodField()
    tResample = serializers.SerializerMethodField()
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

    def get_tResample(self, instance):
        return instance.parent.t_resample

    dfType = serializers.CharField(source="df_type")
    isRestOn = serializers.BooleanField(source="is_rest_on")
    isAugOn = serializers.BooleanField(source="is_aug_on")
    lastReadingTs = serializers.IntegerField(source="last_reading_ts")
    datastreamPk = serializers.IntegerField(source="datastream_id")

    class Meta:
        model = Datafeed
        fields = [
            "id",
            "name",
            "dfType",
            "isRestOn",
            "isAugOn",
            "lastReadingTs",
            "datastreamPk",
            "tResample",
            "dataTypeName",
            "measUnit",
            "aggType",
            "varType",
            "parentId",
        ]
