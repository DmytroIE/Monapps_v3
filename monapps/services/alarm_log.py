from typing import Literal

from django.db.models import Model

from utils.ts_utils import create_dt_from_ts_ms
from utils.db_field_utils import get_instance_full_id


# at the moment - simply put messages into the console
# but later it's necessary to add these records to the db
def add_to_alarm_log(
    type: Literal["ERROR", "WARNING", "INFO"],
    msg: str,
    ts: int,
    instance: Model | str = "Django",
    status: str = ""
):
    if not status:
        status = "IN"

    dt_str = create_dt_from_ts_ms(ts).strftime("%Y/%m/%d %H:%M:%S")
    if isinstance(instance, Model):
        instance_id = get_instance_full_id(instance)
    else:
        instance_id = instance
    print(f"[ALARM LOG]\t[{type}]\t[{status.upper()}]\t{dt_str}\t{instance_id}\t{msg}")
