from django.urls import path
from .views import ListDfReadings

urlpatterns = [
    path("<int:df_pk>/", ListDfReadings.as_view()),
]
