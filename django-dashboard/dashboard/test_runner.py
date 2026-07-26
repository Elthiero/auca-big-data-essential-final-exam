"""
Custom test runner for a database Django does not own.

model_registry, predictions and running_metrics are declared managed=False
because mysql-init/init.sql creates them and the Spark jobs write them —
Django must never migrate or drop them in a real environment. Two separate
consequences follow for tests, and both are handled here:

  1. Django's test runner skips unmanaged models when building the throwaway
     test database, so every query would hit a table that does not exist.
     setup_test_environment flips managed=True for the duration of the run.

  2. run_syncdb only creates tables for apps Django considers unmigrated. An
     app is "migrated" if it merely HAS a migrations package — even an empty
     one containing nothing but __init__.py. So the flip in (1) silently
     achieves nothing if monitor/migrations/ happens to exist, which is easy
     to end up with since Django creates it by default. MIGRATION_MODULES
     maps the app to None, which declares it unmigrated regardless of what is
     on disk. This is deliberately independent of the filesystem: a fix that
     depends on a directory being absent breaks the moment someone unzips an
     archive over their working tree or runs startapp again.
"""

from django.apps import apps
from django.conf import settings
from django.test.runner import DiscoverRunner

UNMIGRATED_APPS = ["monitor"]


class UnmanagedModelTestRunner(DiscoverRunner):
    def setup_test_environment(self, **kwargs):
        self.unmanaged_models = [
            model for model in apps.get_models() if not model._meta.managed
        ]
        for model in self.unmanaged_models:
            model._meta.managed = True

        # Read by the migration loader during create_test_db(), which runs
        # after this hook.
        migration_modules = dict(getattr(settings, "MIGRATION_MODULES", {}))
        migration_modules.update({app: None for app in UNMIGRATED_APPS})
        settings.MIGRATION_MODULES = migration_modules

        super().setup_test_environment(**kwargs)

    def teardown_test_environment(self, **kwargs):
        super().teardown_test_environment(**kwargs)
        for model in self.unmanaged_models:
            model._meta.managed = False
