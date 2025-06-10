from celery import shared_task
from django.db import transaction
from django.conf import settings

from apps.devices.models import Device
from utils.ts_utils import create_now_ts_ms
from utils.update_utils import evaluate_dev_health, derive_health_from_children


@shared_task(bind=True, name="update.devices")
def update_devices(self):

    now_ts = create_now_ts_ms()
    parent_map = {}
    parent_update_fields_map = {}

    with transaction.atomic():
        device_qs = (
            Device.objects.filter(
                next_upd_ts__lte=now_ts,
            )
            .order_by("next_upd_ts")
            .prefetch_related("parent")
            .prefetch_related("datastreams")
            .select_for_update()[: settings.MAX_DEVICES_TO_UPD]
        )

        for dev in device_qs:
            dev_update_fields = set()
            parent = dev.parent
            if parent is not None:
                parent_map[parent.name] = parent
                if parent_update_fields_map.get(parent.name) is None:
                    parent_update_fields_map[parent.name] = set()
            children = list(dev.datastreams.filter(is_enabled=True))

            # evaluate health
            dev_chld_health = derive_health_from_children(children)

            if dev.chld_health != dev_chld_health:
                dev.chld_health = dev_chld_health
                dev_update_fields.add("chld_health")

            dev_health = evaluate_dev_health(dev)
            if dev.health != dev_health:
                dev.health = dev_health
                dev_update_fields.add("health")
                dev.next_upd_ts = settings.MAX_TS_MS
                if parent is not None:
                    if parent.next_upd_ts > now_ts + settings.T_ASSET_UPD_MS:
                        parent.next_upd_ts = now_ts + settings.T_ASSET_UPD_MS
                        parent_update_fields_map[parent.name].add("next_upd_ts")
                    if "health" not in parent.fields_to_update:
                        parent.fields_to_update.append("health")
                        parent_update_fields_map[parent.name].add("fields_to_update")

            dev.next_upd_ts = settings.MAX_TS_MS
            dev_update_fields.add("next_upd_ts")

            dev.save(update_fields=dev_update_fields)

        for dev_ui, parent_update_fields in parent_update_fields_map.items():
            parent = parent_map[dev_ui]
            parent.save(update_fields=parent_update_fields)
