"""
Test suite for the monitoring dashboard.

Scope note: the three tables this app reads (model_registry, predictions,
running_metrics) are managed=False — they are owned by mysql-init/init.sql
and written by the Spark jobs, so Django never migrates them. Creating them
in the throwaway test database is handled once, at runner level, by
dashboard/test_runner.py rather than by hand-written DDL here that would
drift from init.sql.
"""

import datetime

from django.test import TestCase
from django.utils import timezone

from .models import ModelRegistry, Prediction, RunningMetric
from . import views


class RunningStatsTests(TestCase):
    def test_returns_zeros_when_registry_is_empty(self):
        """Before the first micro-batch lands, the dashboard must render
        rather than divide by zero."""
        total, purchase = views.get_running_stats()
        self.assertEqual((total, purchase), (0, 0))

    def test_reads_pre_aggregated_counters(self):
        RunningMetric.objects.create(metric_key="total_predictions", metric_value=5000)
        RunningMetric.objects.create(metric_key="purchase_predicted", metric_value=340)

        total, purchase = views.get_running_stats()

        self.assertEqual(total, 5000)
        self.assertEqual(purchase, 340)


class TrendDataTests(TestCase):
    def _make_prediction(self, minutes_ago, predicted_label=False):
        return Prediction.objects.create(
            user_session=f"session-{minutes_ago}-{predicted_label}",
            event_time=timezone.now(),
            purchase_probability=0.9 if predicted_label else 0.1,
            predicted_label=predicted_label,
            model_version="test",
            scored_at=timezone.now() - datetime.timedelta(minutes=minutes_ago),
        )

    def test_excludes_rows_outside_the_window(self):
        self._make_prediction(minutes_ago=2)
        self._make_prediction(minutes_ago=500)  # well outside a 30 min window

        trend = views.get_trend_data(limit_minutes=30)

        self.assertEqual(sum(bucket["total"] for bucket in trend), 1)

    def test_buckets_are_returned_oldest_first(self):
        """Regression test: slicing an ascending ordering pinned the chart to
        the first minutes of the run forever. The window must track the
        present while still rendering left-to-right in time order."""
        for minutes_ago in (1, 5, 10):
            self._make_prediction(minutes_ago=minutes_ago)

        trend = views.get_trend_data(limit_minutes=30)
        labels = [bucket["minute"] for bucket in trend]

        self.assertEqual(labels, sorted(labels))
        self.assertEqual(len(trend), 3)

    def test_purchase_rate_is_a_percentage_of_the_bucket(self):
        self._make_prediction(minutes_ago=1, predicted_label=True)
        self._make_prediction(minutes_ago=1, predicted_label=False)

        trend = views.get_trend_data(limit_minutes=30)

        self.assertEqual(len(trend), 1)
        self.assertEqual(trend[0]["purchase_rate"], 50.0)


class ApiStatsTests(TestCase):
    def test_returns_valid_json_on_an_empty_database(self):
        """The frontend polls this every 8 seconds starting from page load,
        which is before any prediction exists."""
        response = self.client.get("/api/stats/")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["total_predictions"], 0)
        self.assertEqual(payload["purchase_rate"], 0.0)
        self.assertIsNone(payload["latest_scored_at"])
        self.assertEqual(payload["recent"], [])

    def test_reports_purchase_rate_from_the_counter_table(self):
        RunningMetric.objects.create(metric_key="total_predictions", metric_value=1000)
        RunningMetric.objects.create(metric_key="purchase_predicted", metric_value=68)

        payload = self.client.get("/api/stats/").json()

        self.assertEqual(payload["purchase_rate"], 6.8)


class IndexViewTests(TestCase):
    def test_renders_before_any_model_is_promoted(self):
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.context["champion"])

    def test_surfaces_the_active_champion(self):
        ModelRegistry.objects.create(
            hdfs_path="/models/candidates/20260101_000000",
            version="20260101_000000",
            trained_on="2019-10",
            auc_score=0.91,
            accuracy=0.95,
            is_active=True,
            created_at=timezone.now(),
        )

        response = self.client.get("/")

        self.assertEqual(response.context["champion"].version, "20260101_000000")
