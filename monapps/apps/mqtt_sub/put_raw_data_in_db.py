from collections.abc import Iterable

from django.db import transaction
from django.conf import settings

from apps.datastreams.models import Datastream
from apps.devices.models import Device
from apps.dsreadings.models import (
    DsReading,
    UnusedDsReading,
    InvalidDsReading,
    NonRocDsReading,
    NoDataMarker,
    UnusedNoDataMarker,
)
from utils.prep_ds_readings import prepare_ds_readings
from utils.prep_nd_markers import prep_nodata_markers
from utils.ts_utils import create_now_ts_ms
from utils.update_utils import evaluate_ds_health
from utils.alarm_utils import update_part_of_alarm_map, at_least_one_alarm_in
from utils.sequnce_utils import find_max_ts
from services.alarm_log import add_to_alarm_log
from common.constants import HealthGrades, VariableTypes, DataAggrTypes


def put_raw_data_in_db(dev_ui, payload: dict):
    # 'dev_payload' should look like
    # {"173456980": {"e":{...}, "w":{...}, "i":{...}, "Temperature 1":{...}, "Temperature 2":{...}, ...}
    now_ts = create_now_ts_ms()

    # TODO: split the payload in batches if it is too large and process each batch
    # in a separate "transaction.atomic"

    with transaction.atomic():

        dev = Device.objects.filter(dev_ui=dev_ui).select_for_update().first()

        if dev is None:
            add_to_alarm_log("WARNING",
                             f"No device {dev_ui} in the database",
                             create_now_ts_ms(),
                             instance="MQTT Sub")
            return

        ds_qs = dev.datastreams.filter(is_enabled=True).select_for_update()  # get ACTIVE datastreams only
        # evaluate the qs and lock the datastreams for update
        ds_map: dict[int, Datastream] = {ds.name: ds for ds in ds_qs}

        # start processing the payload
        # -1- replace str timestamps with integers
        int_key_payload = {}
        for k, v in payload.items():
            try:
                ts = int(k)
            except ValueError as e:
                add_to_alarm_log("WARNING",
                                 f"Cannot convert {k} to a timestamp, {e}",
                                 create_now_ts_ms(),
                                 instance="MQTT Sub")
            else:
                int_key_payload[ts] = v
        if len(int_key_payload) == 0:
            add_to_alarm_log("WARNING",
                             "No valid timestamps in the payload",
                             create_now_ts_ms(),
                             instance="MQTT Sub")
            return

        nd_marker_map = {ds.name: set() for ds in ds_qs}
        ds_reading_map = {ds.name: {} for ds in ds_qs}
        int_key_payload = dict(sorted(int_key_payload.items()))  # sort by timestamps
        dev_update_fields = set()
        ds_update_fields_map = {ds.name: set() for ds in ds_qs}

        for ts, row in int_key_payload.items():
            needing_nd_marker_dss = set()
            at_least_one_ds_has_no_errors_and_has_value = False
            # -2- process datastreams
            for ds_name, ds in ds_map.items():
                if (ds_row := row.get(ds_name)) is None:
                    ds_row = {}  # just to ensure '.get' execution

                # -2-1- process ds values
                has_value = False
                new_ds_value = ds_row.get("v")
                if new_ds_value is not None and isinstance(new_ds_value, (int, float)):
                    # add value to the array to be saved later
                    ds_reading_map[ds_name][ts] = new_ds_value
                    has_value = True

                # -2-2- process ds errors
                ds_error_dict_for_ts = ds_row.get("e")
                # even if 'ds_error_dict_for_ts' is None, the alarm map will be processed
                # to ensure 'out' statuses proper assigment
                upd_ds_error_map, is_nd_marker_needed = update_part_of_alarm_map(
                    ds, ds_error_dict_for_ts, ts, "errors", has_value
                )
                if upd_ds_error_map != ds.alarms["errors"]:
                    ds.alarms["errors"] = upd_ds_error_map
                    ds_update_fields_map[ds_name].add("alarms")

                if is_nd_marker_needed:
                    needing_nd_marker_dss.add(ds.name)
                else:
                    if has_value:
                        at_least_one_ds_has_no_errors_and_has_value = True

                # -2-3- process ds warnings
                ds_warning_dict_for_ts = ds_row.get("w")
                upd_ds_warning_map, _ = update_part_of_alarm_map(ds, ds_warning_dict_for_ts, ts, "warnings")
                if upd_ds_warning_map != ds.alarms["warnings"]:
                    ds.alarms["warnings"] = upd_ds_warning_map
                    ds_update_fields_map[ds_name].add("alarms")

                # -2-4- process ds infos
                ds_infos_for_ts = ds_row.get("i")
                if ds_infos_for_ts is not None and isinstance(ds_infos_for_ts, Iterable):
                    for info_str in ds_infos_for_ts:
                        add_to_alarm_log("INFO", info_str, ts, ds, "")

            # -3- Process the device
            # -3-1- process device errors
            dev_error_dict_for_ts = row.get("e")
            upd_dev_error_map, is_nd_marker_needed = update_part_of_alarm_map(
                dev, dev_error_dict_for_ts, ts, "errors", at_least_one_ds_has_no_errors_and_has_value
            )
            if upd_dev_error_map != dev.alarms["errors"]:
                dev.alarms["errors"] = upd_dev_error_map
                dev_update_fields.add("alarms")

            if is_nd_marker_needed:
                needing_nd_marker_dss.update(ds_map.keys())  # on device error all datastreams acquire nd markers

            # -3-2- process device warnings
            dev_warning_dict_for_ts = row.get("w")
            upd_dev_warning_map, _ = update_part_of_alarm_map(dev, dev_warning_dict_for_ts, ts, "warnings")
            if upd_dev_warning_map != dev.alarms["warnings"]:
                dev.alarms["warnings"] = upd_dev_warning_map
                dev_update_fields.add("alarms")

            # -3-3- process device infos
            dev_infos_for_ts = row.get("i")
            if dev_infos_for_ts is not None and isinstance(dev_infos_for_ts, Iterable):
                for info_str in dev_infos_for_ts:
                    add_to_alarm_log("INFO", info_str, ts, dev)

            # -4- add nd markers to the array to be saved later
            for ds_name in needing_nd_marker_dss:
                nd_marker_map[ds_name].add(ts)

        # after the cycle
        # first, process the datastreams
        for ds_name, ds in ds_map.items():
            # -1-1- define ds health
            at_least_one_error_in = at_least_one_alarm_in(ds.alarms["errors"])
            at_least_one_warning_in = at_least_one_alarm_in(ds.alarms["warnings"])
            ds_msg_health = HealthGrades.UNDEFINED
            if at_least_one_error_in:
                ds_msg_health = HealthGrades.ERROR
            elif at_least_one_warning_in:
                ds_msg_health = HealthGrades.WARNING
            if ds.msg_health != ds_msg_health:
                ds.msg_health = ds_msg_health
                ds_update_fields_map[ds_name].add("msg_health")
                ds_health = evaluate_ds_health(ds)
                if ds.health != ds_health:
                    ds.health = ds_health
                    ds_update_fields_map[ds_name].add("health")
                    # as the ds health changed it is necessary to enqueue the parent device update
                    if dev.next_upd_ts > now_ts + settings.T_ASSET_UPD_MS:
                        dev.next_upd_ts = now_ts + settings.T_ASSET_UPD_MS
                        dev_update_fields.add("next_upd_ts")

            # -1-2- create nd markers
            if not ds.is_rbe or (
                ds.data_type.var_type == VariableTypes.CONTINUOUS and ds.data_type.agg_type == DataAggrTypes.AVG
            ):
                nd_markers = []
                unused_nd_markers = []
                # no sense in creating nodata markers for this type of data or if a ds is not RBE
            else:
                nd_markers, unused_nd_markers = prep_nodata_markers(nd_marker_map[ds_name], ds, now_ts)

            # -1-3- create ds readings
            ds_readings, unused_ds_readings, invalid_ds_readings, non_roc_ds_readings = prepare_ds_readings(
                ds_reading_map[ds_name], ds, now_ts
            )

            # -1-4- update 'ts_to_start_with' and 'last_reading_ts'
            new_ts_to_start_with = max(find_max_ts(ds_readings), find_max_ts(nd_markers))
            if new_ts_to_start_with > ds.ts_to_start_with:
                ds.ts_to_start_with = new_ts_to_start_with
                ds_update_fields_map[ds.name].add("ts_to_start_with")

            last_reading_ts = find_max_ts(ds_readings)  # ds_readings - only valid readings
            if ds.last_reading_ts is None or last_reading_ts > ds.last_reading_ts:
                ds.last_reading_ts = last_reading_ts
                ds_update_fields_map[ds.name].add("last_reading_ts")

            # -1-5- for periodic datastreams plan health recalculation right away
            if ds.t_update is not None:
                ds.health_next_eval_ts = now_ts + settings.T_DS_HEALTH_EVAL_MS
                ds_update_fields_map[ds.name].add('health_next_eval_ts')

            # -1-6- finally, save the datastream and readings
            ds.save(update_fields=ds_update_fields_map[ds.name])
            DsReading.objects.bulk_create(ds_readings, batch_size=100, ignore_conflicts=True)
            UnusedDsReading.objects.bulk_create(unused_ds_readings, batch_size=100, ignore_conflicts=True)
            InvalidDsReading.objects.bulk_create(invalid_ds_readings, batch_size=100, ignore_conflicts=True)
            NonRocDsReading.objects.bulk_create(non_roc_ds_readings, batch_size=100, ignore_conflicts=True)
            NoDataMarker.objects.bulk_create(nd_markers, batch_size=100, ignore_conflicts=True)
            UnusedNoDataMarker.objects.bulk_create(unused_nd_markers, batch_size=100, ignore_conflicts=True)

        # -2- then process the device
        # -2-1- device health
        at_least_one_error_in = at_least_one_alarm_in(dev.alarms["errors"])
        at_least_one_warning_in = at_least_one_alarm_in(dev.alarms["warnings"])
        dev_msg_health = HealthGrades.UNDEFINED

        if at_least_one_error_in:
            dev_msg_health = HealthGrades.ERROR
        elif at_least_one_warning_in:
            dev_msg_health = HealthGrades.WARNING
        if dev.msg_health != dev_msg_health:
            dev.msg_health = dev_msg_health
            dev_update_fields.add("msg_health")
            if dev.next_upd_ts > now_ts + settings.T_ASSET_UPD_MS:
                dev.next_upd_ts = now_ts + settings.T_ASSET_UPD_MS
                dev_update_fields.add("next_upd_ts")

        # -2-2- finally, save the device
        dev.save(update_fields=dev_update_fields)
