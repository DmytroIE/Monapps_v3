from rest_framework import serializers


class DfrSerializer(serializers.Serializer):

    t = serializers.IntegerField(source="time")
    v = serializers.SerializerMethodField()
    r = serializers.BooleanField(source="restored")

    def get_v(self, instance):
        return instance.value

    class Meta:
        fields = ["t", "v", "r"]
