import datetime
import json

from django.db.models import Avg, Count, Q
from django.db.models.functions import TruncMinute
from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone

from .models import ModelRegistry, Prediction, RunningMetric


def get_running_stats():
    """
    Queries the running_metrics summary table in O(1) time,
    safely falling back to 0 if keys don't exist yet.
    """
    metrics = {
        item["metric_key"]: item["metric_value"]
        for item in RunningMetric.objects.values("metric_key", "metric_value")
    }
    total = metrics.get("total_predictions", 0)
    purchase = metrics.get("purchase_predicted", 0)
    return total, purchase


def get_trend_data(limit_minutes=30):
    """
    Buckets predictions by minute so the dashboard can show a genuine
    time-series trend of the live stream itself (volume + purchase
    rate over time) — distinct from the model-performance-over-training-
    runs chart, which trends training cycles, not live traffic.

    Two things matter here:
      1. The window is bounded by a scored_at cutoff, which hits the
         idx_scored_at index. Without it, this GROUP BY scans the entire
         predictions table on every 8-second poll — fine at 10k rows,
         not fine at 10M.
      2. Ordering is DESC before slicing, then reversed for display.
         Slicing an ASC ordering returns the OLDEST n buckets, which
         means the chart freezes on the first n minutes of the run and
         never advances.
    """
    cutoff = timezone.now() - datetime.timedelta(minutes=limit_minutes)

    buckets = (
        Prediction.objects.filter(scored_at__gte=cutoff)
        .annotate(minute=TruncMinute("scored_at"))
        .values("minute")
        .annotate(
            total=Count("id"),
            purchases=Count("id", filter=Q(predicted_label=True)),
        )
        .order_by("-minute")[:limit_minutes]
    )
    buckets = sorted(buckets, key=lambda b: b["minute"] or datetime.datetime.min)

    return [
        {
            "minute": b["minute"].strftime("%m-%d %H:%M") if b["minute"] else "",
            "total": b["total"],
            "purchases": b["purchases"],
            "purchase_rate": (
                round(b["purchases"] / b["total"] * 100, 1) if b["total"] else 0
            ),
        }
        for b in buckets
    ]


def index(request):
    champion = (
        ModelRegistry.objects.filter(is_active=True).order_by("-created_at").first()
    )

    history = list(
        ModelRegistry.objects.order_by("created_at").values(
            "version",
            "trained_on",
            "auc_score",
            "accuracy",
            "precision_purchase",
            "recall_purchase",
            "decision_threshold",
            "is_active",
            "created_at",
        )
    )
    all_models = ModelRegistry.objects.order_by("-created_at")

    recent_predictions = Prediction.objects.order_by("-scored_at")[:20]

    # total_predictions = Prediction.objects.count()
    # purchase_predicted = Prediction.objects.filter(predicted_label=True).count()
    # no_purchase_predicted = total_predictions - purchase_predicted
    # avg_probability = Prediction.objects.aggregate(avg=Avg("purchase_probability"))["avg"] or 0.0
    # purchase_rate = (purchase_predicted / total_predictions * 100) if total_predictions else 0.0

    total_predictions, purchase_predicted = get_running_stats()
    no_purchase_predicted = total_predictions - purchase_predicted
    purchase_rate = (
        (purchase_predicted / total_predictions * 100) if total_predictions else 0.0
    )

    # Bounded to a recent window rather than the full table. An unbounded
    # Avg() is a full scan of predictions; it is only run on page load
    # rather than on every poll, but at multi-million row scale that is
    # still a multi-second render. The recent-window average is also the
    # more useful number operationally — it reflects what the currently
    # active model is doing, not an average smeared across every model
    # version ever promoted.
    avg_window_start = timezone.now() - datetime.timedelta(minutes=30)
    avg_probability = (
        Prediction.objects.filter(scored_at__gte=avg_window_start).aggregate(
            avg=Avg("purchase_probability")
        )["avg"]
        or 0.0
    )

    context = {
        "champion": champion,
        # json.dumps with default=str handles the datetime objects from
        # .values() — Chart.js on the frontend just needs plain strings.
        "history_json": json.dumps(history, default=str),
        "trend_json": json.dumps(get_trend_data()),
        "all_models": all_models,
        "recent_predictions": recent_predictions,
        "total_predictions": total_predictions,
        "purchase_predicted": purchase_predicted,
        "no_purchase_predicted": no_purchase_predicted,
        "purchase_rate": purchase_rate,
        "avg_probability": avg_probability,
    }
    return render(request, "monitor/index.html", context)


def api_stats(request):
    """
    Polled by the dashboard's frontend JS every few seconds so the
    summary numbers and recent-predictions table update live without a
    full page reload.
    """
    total, purchase = get_running_stats()
    latest = Prediction.objects.order_by("-scored_at").first()

    recent = list(
        Prediction.objects.order_by("-scored_at")[:20].values(
            "user_session",
            "purchase_probability",
            "predicted_label",
            "model_version",
            "scored_at",
        )
    )

    return JsonResponse(
        {
            "total_predictions": total,
            "purchase_predicted": purchase,
            "purchase_rate": round(purchase / total * 100, 2) if total else 0.0,
            "latest_scored_at": latest.scored_at.isoformat() if latest else None,
            "recent": recent,
            "trend": get_trend_data(),
        }
    )
