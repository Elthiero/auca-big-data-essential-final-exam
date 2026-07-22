from django.urls import path

from monitor import views

urlpatterns = [
    path("", views.index, name="index"),
    path("api/stats/", views.api_stats, name="api_stats"),
]