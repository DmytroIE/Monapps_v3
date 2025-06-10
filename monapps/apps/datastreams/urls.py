from django.urls import path
from .views import DsRetrieve

urlpatterns = [
    path("<int:pk>/", DsRetrieve.as_view()),
]
