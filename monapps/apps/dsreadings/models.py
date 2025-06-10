from common.abstract_classes import AnyDsReading, AnyNoDataMarker


class DsReading(AnyDsReading):

    class Meta:
        db_table = "ds_readings"

    short_name = "DSR"


class UnusedDsReading(AnyDsReading):
    class Meta:
        db_table = "unused_ds_readings"

    short_name = "Unused DSR"


class InvalidDsReading(AnyDsReading):

    class Meta:
        db_table = "invalid_ds_readings"

    short_name = "Invalid DSR"


class NonRocDsReading(AnyDsReading):

    class Meta:
        db_table = "nonroc_ds_readings"

    short_name = "DSRNon-ROC DSR"


class NoDataMarker(AnyNoDataMarker):

    class Meta:
        db_table = "nd_markers"

    short_name = "NDM"


class UnusedNoDataMarker(AnyNoDataMarker):

    class Meta:
        db_table = "unused_nd_markers"

    short_name = "Unused NDM"
