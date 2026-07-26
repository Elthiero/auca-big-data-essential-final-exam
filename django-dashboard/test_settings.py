"""
Test-only settings overlay.

Runs the dashboard test suite against an in-memory SQLite database so tests
need no running MySQL container, no fixtures and no network. Everything else
— installed apps, URLs, templates, the unmanaged-model test runner — is
inherited unchanged from dashboard.settings.

    python manage.py test monitor --settings=test_settings

To run the suite against real MySQL instead (which additionally validates the
schema in mysql-init/init.sql), drop the override and use the default
settings from inside the container:

    docker compose exec django-dashboard python manage.py test monitor

That path requires the database user to hold CREATE privileges, since Django
builds a separate test_<dbname> database.
"""

from dashboard.settings import *  # noqa: F401,F403

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}
