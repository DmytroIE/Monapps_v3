from rest_framework.views import APIView
from rest_framework.response import Response
from .serializers import DfrSerializer
from apps.dfreadings.models import DfReading


class ListDfReadings(APIView):

    def get(self, request, **kwargs):
        df_pk = kwargs.get("df_pk")
        qs = DfReading.objects.filter(datafeed_id=df_pk).order_by("time")

        if "gt" in self.request.query_params:
            gt = int(self.request.query_params.get("gt"))
            qs = qs.filter(time__gt=gt)
        elif "gte" in self.request.query_params:
            gte = int(self.request.query_params.get("gte"))
            qs = qs.filter(time__gte=gte)

        if "lte" in self.request.query_params:
            lte = int(self.request.query_params.get("lte"))
            qs = qs.filter(time__lte=lte)
        dfreadings = DfrSerializer(qs, many=True)
        df_id = f"datafeed {df_pk}"
        df_dict = {df_id: {"dfReadings": dfreadings.data}}

        return Response(df_dict)
