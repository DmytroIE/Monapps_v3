from celery import shared_task
from django.db import transaction
from django.conf import settings

from apps.datastreams.models import Datastream
from common.constants import HealthGrades
from utils.ts_utils import create_now_ts_ms
from utils.update_utils import evaluate_ds_health


@shared_task(bind=True, name="update.periodic_ds_health")
def update_periodic_ds_health(self):
    '''
    This task works only for periodic datastreams (the datastreams that have 't_update' != null)
    '''
    now_ts = create_now_ts_ms()
    dev_map = {}
    dev_update_fields_map = {}

    with transaction.atomic():
        # bring MAX_DS_TO_HEALTH_PROC active periodic datastream instances, the rest will be processed further
        ds_qs = (
            Datastream.objects.filter(
                health_next_eval_ts__lte=now_ts,
            )
            .filter(is_enabled=True)
            .exclude(t_update__isnull=True)
            .order_by("health_next_eval_ts")
            .prefetch_related('parent')
            .select_for_update()[: settings.MAX_DS_TO_HEALTH_PROC]
        )

        for ds in ds_qs:  # this will evaluate the qs and lock the datastreams for update
            ds_update_fields = set()
            dev = ds.parent
            dev_map[dev.dev_ui] = dev
            if dev_update_fields_map.get(dev.dev_ui) is None:
                dev_update_fields_map[dev.dev_ui] = set()

            # evaluate health
            ds_nd_health = HealthGrades.UNDEFINED
            if ds.last_reading_ts is None:
                if now_ts - ds.created_ts > ds.t_nd_health_error:  # TODO: from 'enabled' not from 'created'?
                    ds_nd_health = HealthGrades.ERROR
                else:
                    ds_nd_health = HealthGrades.UNDEFINED
            else:
                if now_ts - ds.last_reading_ts > ds.t_nd_health_error:
                    ds_nd_health = HealthGrades.ERROR
                else:
                    ds_nd_health = HealthGrades.OK
            if ds.nd_health != ds_nd_health:
                ds.nd_health = ds_nd_health
                ds_update_fields.add("nd_health")
                ds_health = evaluate_ds_health(ds)
                if ds.health != ds_health:
                    ds.health = ds_health
                    ds_update_fields.add("health")
                    # as the ds health changed it is necessary to enqueue the parent device update
                    if dev.next_upd_ts > now_ts + settings.T_ASSET_UPD_MS:
                        dev.next_upd_ts = now_ts + settings.T_ASSET_UPD_MS
                        dev_update_fields_map[dev.dev_ui].add('next_upd_ts')

            ds.health_next_eval_ts = now_ts + max(
                settings.T_DS_HEALTH_EVAL_MS, ds.t_update * settings.NEXT_EVAL_MARGIN_COEF
            )
            ds_update_fields.add('health_next_eval_ts')

            # save the changes
            ds.save(update_fields=ds_update_fields)

        for dev_ui, dev_update_fields in dev_update_fields_map.items():
            dev = dev_map[dev_ui]
            dev.save(update_fields=dev_update_fields)
