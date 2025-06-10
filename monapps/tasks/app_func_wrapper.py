import traceback
from collections.abc import Iterable
from celery import shared_task

from django.db import transaction, IntegrityError
from django.conf import settings
from django_celery_beat.models import PeriodicTask

from apps.applications.models import Application
from apps.dfreadings.models import DfReading

from common.constants import HealthGrades, STATUS_FIELD_NAME, CURR_STATE_FIELD_NAME
from utils.ts_utils import create_now_ts_ms
from utils.prep_all_df_readings import create_all_df_readings
from utils.sequnce_utils import find_instance_with_max_attr
from utils.alarm_utils import update_part_of_alarm_map
from services.alarm_log import add_to_alarm_log
from app_functions.app_functions import app_function_map


@shared_task(bind=True, name="evaluate.app_func")
def app_func_wrapper(self) -> None:

    # The code below is based on the assumption that:
    # - new df readings for all datafeeds of the application are created only within this function
    # - no new df readings were created after the previous invocation of this function

    try:
        task = PeriodicTask.objects.get(name=self.request.periodic_task_name)
    except (PeriodicTask.DoesNotExist, PeriodicTask.MultipleObjectsReturned):
        print(f"Cannon get the task instance for the name '{self.request.periodic_task_name}'")
        return
    app: Application | None = getattr(task, "application", None)  # a backward relation

    if app is None:
        print(f"No application for the task '{task.name}'")
        return

    update_map = {}
    app_update_fields = set()
    excep_health = HealthGrades.UNDEFINED
    health_from_app = HealthGrades.UNDEFINED

    if app.is_enabled:
        # first, create new df readings - this function has its own 'transaction.atomic'
        create_all_df_readings(app)

    with transaction.atomic():
        app = Application.objects.select_for_update().get(pk=app.pk)
        if app.is_enabled:
            try:
                with transaction.atomic():
                    native_df_qs = app.get_native_df_qs().select_for_update()
                    native_df_map = {df.name: df for df in native_df_qs}
                    derived_df_qs = app.get_derived_df_qs().select_for_update()
                    derived_df_map = {df.name: df for df in derived_df_qs}
                    task = PeriodicTask.objects.select_for_update().get(pk=task.pk)

                    app_func_cluster = app_function_map.get(app.type.func_name)
                    if app_func_cluster is None:
                        raise Exception(f"No {app.type.func_name} in the app function map")

                    app_func = app_func_cluster.get(app.func_version)
                    if app_func is None:
                        raise Exception(
                            f"No version {app.func_version} for {app.type.func_name} in the app function map"
                        )

                    # now it is possible to do the main thing - to do some application logic
                    derived_df_readings, update_map = app_func(app, native_df_map, derived_df_map)

                    # -1- update derived datafeeds and save derived df readings
                    for df_name, df_row in derived_df_readings.items():
                        df = df_row["df"]
                        new_df_readings = df_row["new_df_readings"]
                        latest_dfr = find_instance_with_max_attr(new_df_readings)
                        if latest_dfr is not None:  # the same as 'if len(new_df_readings) > 0'
                            max_rts = latest_dfr.time
                            val_at_max_rts = latest_dfr.value
                            DfReading.objects.bulk_create(new_df_readings)  # FIXME: join in one array and save once?
                            if df.last_reading_ts is None or max_rts > df.last_reading_ts:
                                df.last_reading_ts = max_rts
                                df.save()

                            # -1-1- update status
                            if df_name == STATUS_FIELD_NAME and app.type.has_status:
                                if app.last_status_update_ts is None or max_rts > app.last_status_update_ts:
                                    app.last_status_update_ts = max_rts
                                    app_update_fields.add("last_status_update_ts")
                                    if app.status != val_at_max_rts:
                                        app.status = val_at_max_rts
                                        app_update_fields.add("status")
                                        # TODO: add a message to the alarm log
                            # -1-2- update curr state
                            if df_name == CURR_STATE_FIELD_NAME and app.type.has_curr_state:
                                if app.last_curr_state_update_ts is None or max_rts > app.last_curr_state_update_ts:
                                    app.last_curr_state_update_ts = max_rts
                                    app_update_fields.add("last_curr_state_update_ts")
                                    if app.curr_state != val_at_max_rts:
                                        app.curr_state = val_at_max_rts
                                        app_update_fields.add("curr_state")
                                        # TODO: add a message to the alarm log

                    # -2- update the task if needed
                    if (is_catching_up := update_map.get("is_catching_up")) is not None:
                        if is_catching_up and not app.is_catching_up:
                            task.interval = app.catch_up_interval
                            task.save()
                            app.is_catching_up = is_catching_up
                            app_update_fields.add("is_catching_up")
                        elif not is_catching_up and app.is_catching_up:
                            task.interval = app.invoc_interval
                            task.save()
                            app.is_catching_up = is_catching_up
                            app_update_fields.add("is_catching_up")

                    # -3- update the cursor position
                    cursor_ts = app.cursor_ts
                    if (ts := update_map.get("cursor_ts")) is not None:
                        cursor_ts = ts
                        if cursor_ts > app.cursor_ts:
                            app.cursor_ts = cursor_ts
                            app_update_fields.add("cursor_ts")

                    # -4- health from the app function
                    if (h := update_map.get("health")) is not None:
                        # HealthGrades.OK is not used for this type of health
                        health_from_app = h if h != HealthGrades.OK else HealthGrades.UNDEFINED

                    # -5- process alarms
                    if (alarm_payload := update_map.get("alarm_payload")) is not None:
                        for ts, row in alarm_payload.items():
                            app_error_dict_for_ts = row.get("e")
                            upd_app_error_map, _ = update_part_of_alarm_map(app, app_error_dict_for_ts, ts, "errors")
                            if app.alarms["errors"] != upd_app_error_map:
                                app.alarms["errors"] = upd_app_error_map
                                app_update_fields.add("alarms")

                            app_warning_dict_for_ts = row.get("w")
                            upd_app_warning_map, _ = update_part_of_alarm_map(
                                app, app_warning_dict_for_ts, ts, "warnings"
                            )
                            if app.alarms["warnings"] != upd_app_warning_map:
                                app.alarms["warnings"] = upd_app_warning_map
                                app_update_fields.add("alarms")

                            app_infos_for_ts = row.get("i")
                            if app_infos_for_ts is not None and isinstance(app_infos_for_ts, Iterable):
                                for info_str in app_infos_for_ts:
                                    add_to_alarm_log("INFO", info_str, ts, app)

                    add_to_alarm_log("INFO", "App function was executed", create_now_ts_ms(), instance=app)

            except IntegrityError:
                print(
                    """An attempt to rewrite existing df readings detected,
                        all the results of the app function evaluation will be discarded"""
                )
                excep_health = HealthGrades.ERROR
            except Exception as e:
                excep_health = HealthGrades.ERROR
                print(f"Error happened while executing app function, {e}\n{traceback.format_exc()}")

        # this part is performed regardles of the 'app.is_enabled' value
        now_ts = create_now_ts_ms()
        if app.type.has_status:
            if app.last_status_update_ts is not None:
                is_status_stale = now_ts - app.last_status_update_ts > app.t_status_stale
            else:
                is_status_stale = now_ts - app.created_ts > app.t_status_stale

            if is_status_stale != app.is_status_stale:
                app.is_status_stale = is_status_stale
                app_update_fields.add("is_status_stale")
                # TODO: add a message to the alarm log

        if app.type.has_curr_state:
            if app.last_curr_state_update_ts is not None:
                is_curr_state_stale = now_ts - app.last_curr_state_update_ts > app.t_curr_state_stale
            else:
                is_curr_state_stale = now_ts - app.created_ts > app.t_status_stale

            if is_curr_state_stale != app.is_curr_state_stale:
                app.is_curr_state_stale = is_curr_state_stale
                app_update_fields.add("is_curr_state_stale")
                # TODO: add a message to the alarm log

        # health based on the cursor timestamp
        if app.is_enabled and not app.is_catching_up:
            if now_ts - app.cursor_ts > app.t_health_error:
                cs_health = HealthGrades.ERROR
            else:
                cs_health = HealthGrades.OK
        else:
            cs_health = HealthGrades.UNDEFINED

        health = max(cs_health, health_from_app, excep_health)
        if app.health != health:
            app.health = health
            app_update_fields.add("health")
            # TODO: add a message to the alarm log

        # finally, save the app
        app.save(update_fields=app_update_fields)

    # Update parent
    parent = app.parent
    if parent is not None:
        parent_update_fields = set()
        if "status" in app_update_fields or "is_status_stale" in app_update_fields:
            if "status" not in parent.fields_to_update:
                parent.fields_to_update.append("status")
                parent_update_fields.add("fields_to_update")

        if "curr_state" in app_update_fields or "is_curr_state_stale" in app_update_fields:
            if "curr_state" not in parent.fields_to_update:
                parent.fields_to_update.append("curr_state")
                parent_update_fields.add("fields_to_update")

        if "health" in app_update_fields:
            if "health" not in parent.fields_to_update:
                parent.fields_to_update.append("health")
                parent_update_fields.add("fields_to_update")

        # if any of app's 'status', 'curr_state' or 'health' changed
        # it is necessary to enqueue the parent update procedure
        if len(parent_update_fields) > 0:
            if parent.next_upd_ts > now_ts + settings.T_ASSET_UPD_MS:
                parent.next_upd_ts = now_ts + settings.T_ASSET_UPD_MS
                parent_update_fields.add("next_upd_ts")
            parent.save(update_fields=parent_update_fields)
