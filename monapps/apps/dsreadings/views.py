from rest_framework.views import APIView
from rest_framework.response import Response
from .serializers import DsrSerializer
from .models import (
    DsReading,
    UnusedDsReading,
    NonRocDsReading,
    InvalidDsReading,
    NoDataMarker,
    UnusedNoDataMarker
)


key_model_map = {
    "dsReadings": DsReading,
    "invDsReadings": InvalidDsReading,
    "unusDsReadings": UnusedDsReading,
    "norcDsReadings": NonRocDsReading,
    "ndMarkers": NoDataMarker,
    "unusNdMarkers": UnusedNoDataMarker,
}


class ListDsReadings(APIView):

    def get(self, request, **kwargs):
        ds_pk = kwargs.get("ds_pk")
        gt = lte = gte = None
        if "gt" in self.request.query_params:
            gt = int(self.request.query_params.get("gt"))
        if "gte" in self.request.query_params:
            gte = int(self.request.query_params.get("gte"))
        if "lte" in self.request.query_params:
            lte = int(self.request.query_params.get("lte"))

        ds_id = f"datastream {ds_pk}"
        ds_dict = {ds_id: {}}

        for key, model in key_model_map.items():
            qs = model.objects.filter(datastream_id=ds_pk).order_by("time")

            if gte is not None:
                qs = qs.filter(time__gte=gte)
            elif gt is not None:
                qs = qs.filter(time__gt=gt)

            if lte is not None:
                qs = qs.filter(time__lte=lte)
            readings = DsrSerializer(list(qs), many=True)
            ds_dict[ds_id][key] = readings.data

        return Response(ds_dict)
