from django.urls import path
from .views import ListDsReadings

urlpatterns = [
    path("<int:ds_pk>/", ListDsReadings.as_view()),
]
