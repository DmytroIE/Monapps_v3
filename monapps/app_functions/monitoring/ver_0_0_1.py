from apps.applications.models import Application
from apps.datafeeds.models import Datafeed
from common.complex_types import AppFuncReturn, DerivedDfReadingMap, UpdateMap
from utils.app_func_utils import get_end_rts


def monitoring_0_0_1(
    app: Application, native_df_map: dict[str, Datafeed], derived_df_map: dict[str, Datafeed]
) -> AppFuncReturn:
    """
    Does nothing except moving the app cursor.
    Can be used for pure monitoring purposes, when only proper resampling of datastream readings is needed.
    """
    print("We are in 'monitoring_0_0_1'")

    start_rts = app.cursor_ts
    num_df_to_process = len(native_df_map)
    end_rts, is_catching_up = get_end_rts(native_df_map.values(), app.t_resample, start_rts, num_df_to_process)

    update_map: UpdateMap = {"cursor_ts": end_rts}
    derived_df_reading_map: DerivedDfReadingMap = {}
    return derived_df_reading_map, update_map
