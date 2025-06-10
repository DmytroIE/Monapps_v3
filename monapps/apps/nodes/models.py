from django.db import models
from treebeard.mp_tree import MP_Node
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType


def limit_content_type():
    return (
        models.Q(app_label="assets", model="asset")
        | models.Q(app_label="devices", model="device")
        | models.Q(app_label="applications", model="application")
        | models.Q(app_label="datastreams", model="datastream")
        | models.Q(app_label="datafeeds", model="datafeed")
    )


class Node(MP_Node):

    own_name = models.CharField(default="", max_length=200, unique=False, blank=True)
    content_type = models.ForeignKey(
        ContentType, blank=True, null=True, on_delete=models.SET_NULL, limit_choices_to=limit_content_type
    )
    object_id = models.PositiveIntegerField(blank=True, null=True)
    content_object = GenericForeignKey("content_type", "object_id")

    def __str__(self):
        return self.name

    @property
    def name(self):
        if self.content_object:
            return f"Node {self.pk}: {self.content_object}"
        else:
            return f"Node {self.pk}: No object attached"
