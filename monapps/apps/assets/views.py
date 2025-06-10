from .models import Asset
from .serializers import AssetSerializer
from rest_framework import generics


class AssetRetrieve(generics.RetrieveAPIView):
    queryset = Asset.objects.all()
    serializer_class = AssetSerializer
