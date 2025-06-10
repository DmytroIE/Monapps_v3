from rest_framework import serializers

from .models import Application
from utils.db_field_utils import get_parent_id, get_instance_full_id


class AppSerializer(serializers.ModelSerializer):

    id = serializers.SerializerMethodField()
    parentId = serializers.SerializerMethodField()

    def get_id(self, instance):
        return get_instance_full_id(instance)

    def get_parentId(self, instance):
        return get_parent_id(instance)

    tResample = serializers.IntegerField(source="t_resample")
    cursorTs = serializers.IntegerField(source="cursor_ts")
    isCatchingUp = serializers.BooleanField(source="is_catching_up")
    isEnabled = serializers.BooleanField(source="is_enabled")
    isStatusStale = serializers.BooleanField(source="is_status_stale")
    isCurrStateStale = serializers.BooleanField(source="is_curr_state_stale")
    currState = serializers.IntegerField(source="curr_state")
    lastStatusUpdateTs = serializers.IntegerField(source="last_status_update_ts")
    lastCurrStateUpdateTs = serializers.IntegerField(source="last_curr_state_update_ts")
    statusUse = serializers.IntegerField(source="status_use")
    currStateUse = serializers.IntegerField(source="curr_state_use")

    class Meta:
        model = Application
        fields = [
            "id",
            "name",
            "tResample",
            "cursorTs",
            "isCatchingUp",
            "isEnabled",
            "isStatusStale",
            "isCurrStateStale",
            "status",
            "currState",
            "lastStatusUpdateTs",
            "lastCurrStateUpdateTs",
            "statusUse",
            "currStateUse",
            "alarms",
            "health",
            "parentId",
        ]
