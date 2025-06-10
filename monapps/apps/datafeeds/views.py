from .models import Datafeed
from .serializers import DfSerializer
from rest_framework import generics


class DfRetrieve(generics.RetrieveAPIView):
    queryset = Datafeed.objects.all()
    serializer_class = DfSerializer
