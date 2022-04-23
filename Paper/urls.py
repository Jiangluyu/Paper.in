"""Paper URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from PaperInfo import views

urlpatterns = [
    path('', views.index),
    path('visual/', views.visual),
    path('visual/', views.visual, name="visualization"),
    path('search/', views.search),
    path('search/', views.search, name="search-content"),
    path('relative/', views.relative),
    path('relative/', views.relative, name="relative-content"),
    path('graph/', views.graph),
    path('graph/', views.graph, name="graph-content"),
    path('admin/', admin.site.urls),
]
