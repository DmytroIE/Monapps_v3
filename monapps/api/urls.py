from django.urls import path, include


urlpatterns = [
    path("assets/", include("apps.assets.urls")),
    path("devices/", include("apps.devices.urls")),
    path("applications/", include("apps.applications.urls")),
    path("datastreams/", include("apps.datastreams.urls")),
    path("datafeeds/", include("apps.datafeeds.urls")),
    path("dfreadings/", include("apps.dfreadings.urls")),
    path("dsreadings/", include("apps.dsreadings.urls")),
    path("nodes/", include("apps.nodes.urls")),
]
