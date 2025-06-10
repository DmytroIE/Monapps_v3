from django.urls import path
from .views import AppRetrieve

urlpatterns = [
    path("<int:pk>/", AppRetrieve.as_view()),
]
