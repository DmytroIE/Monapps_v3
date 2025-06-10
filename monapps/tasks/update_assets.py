from celery import shared_task
from django.db import transaction
from django.conf import settings

from apps.assets.models import Asset
from utils.ts_utils import create_now_ts_ms
from utils.update_utils import update_func_by_property_map


@shared_task(bind=True, name="update.assets")
def update_assets(self):

    now_ts = create_now_ts_ms()
    parent_map = {}
    parent_update_fields_map = {}

    with transaction.atomic():
        asset_qs = (
            Asset.objects.filter(
                next_upd_ts__lte=now_ts,
            )
            .order_by("next_upd_ts")
            .prefetch_related("parent")
            .prefetch_related("assets")
            .prefetch_related("applications")
            .prefetch_related("devices")
            .select_for_update()[: settings.MAX_ASSETS_TO_UPD]
        )
        for asset in asset_qs:
            asset_update_fields = set()
            parent = asset.parent
            if parent is not None:
                parent_map[parent.name] = parent
                parent_update_fields_map[parent.name] = set()
            children = [
                *asset.applications.all(),
                *asset.devices.all(),
                *asset.assets.all(),
            ]

            if len(asset.fields_to_update) > 0:  # it definitely should be >0,
                # otherwise who could initiate the update process for this asset
                # without injecting at least one field into it 'fields_to_update'?

                for field_name in asset.fields_to_update:
                    func = update_func_by_property_map[field_name]
                    new_value = func(children)
                    if new_value != getattr(asset, field_name, None):
                        setattr(asset, field_name, new_value)
                        asset_update_fields.add(field_name)
                        if field_name == "status":
                            asset.last_status_update_ts = now_ts
                            asset_update_fields.add("last_status_update_ts")
                        if field_name == "curr_state":
                            asset.last_curr_state_update_ts = now_ts
                            asset_update_fields.add("last_curr_state_update_ts")

                        if parent is not None:
                            if parent.next_upd_ts > now_ts + settings.T_ASSET_UPD_MS:
                                parent.next_upd_ts = now_ts + settings.T_ASSET_UPD_MS
                                parent_update_fields_map[parent.name].add("next_upd_ts")
                            if field_name not in parent.fields_to_update:
                                parent.fields_to_update.append(field_name)
                                parent_update_fields_map[parent.name].add("fields_to_update")

                asset.fields_to_update = []
                asset_update_fields.add("fields_to_update")

            asset.next_upd_ts = settings.MAX_TS_MS
            asset_update_fields.add("next_upd_ts")

            asset.save(update_fields=asset_update_fields)

        for parent_name, parent_update_fields in parent_update_fields_map.items():
            parent = parent_map[parent_name]
            parent.save(update_fields=parent_update_fields)
