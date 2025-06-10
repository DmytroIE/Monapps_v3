from collections.abc import Iterable

from apps.datastreams.models import Datastream
from apps.dsreadings.models import NoDataMarker, UnusedNoDataMarker


def prep_nodata_markers(
    tss: Iterable[int], ds: Datastream, now: int
) -> tuple[list[NoDataMarker], list[UnusedNoDataMarker]]:

    nd_markers = []
    unused_nd_markers = []

    from_ts = ds.ts_to_start_with

    for ts in tss:
        if ts > from_ts and ts < now:
            nd_markers.append(NoDataMarker(time=ts, datastream=ds))
        else:
            unused_nd_markers.append(UnusedNoDataMarker(time=ts, datastream=ds))

    return nd_markers, unused_nd_markers
