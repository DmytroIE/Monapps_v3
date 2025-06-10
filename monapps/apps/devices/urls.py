from django.urls import path
from .views import DevRetrieve

urlpatterns = [
    path("<int:pk>/", DevRetrieve.as_view()),
]
