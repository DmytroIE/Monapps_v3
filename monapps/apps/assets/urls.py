from django.urls import path
from .views import AssetRetrieve

urlpatterns = [
    path("<int:pk>/", AssetRetrieve.as_view()),
]
