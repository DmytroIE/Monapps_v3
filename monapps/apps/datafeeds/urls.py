from django.urls import path
from .views import DfRetrieve

urlpatterns = [
    path("<int:pk>/", DfRetrieve.as_view()),
]
