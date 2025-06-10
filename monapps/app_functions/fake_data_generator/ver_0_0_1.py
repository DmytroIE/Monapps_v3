import random
import time
from apps.applications.models import Application
from apps.datafeeds.models import Datafeed
from apps.dfreadings.models import DfReading
from utils.ts_utils import floor_timestamp, create_now_ts_ms
from common.complex_types import AppFuncReturn, DerivedDfReadingMap, UpdateMap
from common.constants import STATUS_FIELD_NAME, CURR_STATE_FIELD_NAME


def fake_data_generator_0_0_1(
    app: Application, native_df_map: dict[str, Datafeed], derived_df_map: dict[str, Datafeed]
) -> AppFuncReturn:
    """Used as a generator of different status and current state values for testing the update algorithms.
    Also, sometimes can generate exceptions to test the exception handling in the wrapper"
    """
    print("We are in 'fake_data_generator_0_0_1'")

    status_df = derived_df_map[STATUS_FIELD_NAME]
    curr_state_df = derived_df_map[CURR_STATE_FIELD_NAME]

    update_map: UpdateMap = {}
    derived_df_reading_map: DerivedDfReadingMap = {
        STATUS_FIELD_NAME: {"df": status_df, "new_df_readings": []},
        CURR_STATE_FIELD_NAME: {"df": curr_state_df, "new_df_readings": []},
    }
    alarm_payload = {}  # {1734567890123: {"e": {"Wrong data":{}, "Something else": {"st": "in"}}, "w": {...}}, ...}

    prob_exeption = app.settings.get("prob_exeption", 0.5)
    prob_status_calc_omitted = app.settings.get("prob_status_calc_omitted", 0.5)
    prob_curr_state_calc_omitted = app.settings.get("prob_curr_state_calc_omitted", 0.5)

    now_ts = create_now_ts_ms()

    # imitate synchronous delay
    time.sleep(random.randrange(1, 4))

    var = None
    if random.random() < prob_exeption:
        var = 1 / 0  # generate an exception

    rts = floor_timestamp(now_ts, app.t_resample)

    if random.random() < prob_status_calc_omitted and rts != app.cursor_ts:
        # 'rts != app.cursor_ts' is needed to protect creating df readings with the same ts
        # if the interval between function invocations < t_resample
        status = random.randint(0, 3)
        dfr = DfReading(time=rts, value=status, datafeed=status_df, restored=False)
        derived_df_reading_map[STATUS_FIELD_NAME]["new_df_readings"].append(dfr)
        update_map["cursor_ts"] = rts

    if random.random() < prob_curr_state_calc_omitted and rts != app.cursor_ts:
        # 'rts != app.cursor_ts' is needed to protect creating df readings with the same ts
        # if the interval between function invocations < t_resample
        curr_state = random.randint(0, 3)
        dfr = DfReading(time=rts, value=curr_state, datafeed=curr_state_df, restored=False)
        derived_df_reading_map[CURR_STATE_FIELD_NAME]["new_df_readings"].append(dfr)
        update_map["cursor_ts"] = rts

    return derived_df_reading_map, update_map
