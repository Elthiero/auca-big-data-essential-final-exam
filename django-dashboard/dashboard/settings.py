"""
Minimal Django settings for the Big Data Essentials dashboard.

Deliberately excludes django.contrib.auth/admin/sessions/messages —
this dashboard is read-only and has no login, so none of Django's
built-in user/session machinery (and its migrations) are needed.
That keeps this container's only database dependency the two tables
mysql-init/init.sql already owns (model_registry, predictions) —
Django never runs migrate against this database at all.
"""

import os
from pathlib import Path

import pymysql

# PyMySQL as a drop-in for MySQLdb — Django's mysql backend expects the
# mysqlclient (MySQLdb) API. This avoids needing mysqlclient's C build
# dependencies (libmysqlclient-dev + a compiler) in the image, matching
# the lightweight approach used by every other service in this project.
pymysql.install_as_MySQLdb()

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get(
    "DJANGO_SECRET_KEY",
    "dev-only-insecure-key-change-before-any-real-deployment",
)

DEBUG = os.environ.get("DJANGO_DEBUG", "True").lower() == "true"

# Course-project scope: dashboard is only reachable inside the docker
# network / via localhost:8000 on the host, so a wildcard is fine here.
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.staticfiles",
    "monitor",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "dashboard.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
            ],
        },
    },
]

WSGI_APPLICATION = "dashboard.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": os.environ.get("MYSQL_DB", "model_registry"),
        "USER": os.environ.get("MYSQL_USER", "bigdata_user"),
        "PASSWORD": os.environ.get("MYSQL_PASSWORD", "changeme"),
        "HOST": os.environ.get("MYSQL_HOST", "mysql"),
        "PORT": os.environ.get("MYSQL_PORT", "3306"),
    }
}

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"