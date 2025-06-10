from .models import Device
from .serializers import DevSerializer
from rest_framework import generics


class DevRetrieve(generics.RetrieveAPIView):
    queryset = Device.objects.all()
    serializer_class = DevSerializer
