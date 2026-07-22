from django.db import models


class ModelRegistry(models.Model):
    """
    Maps onto the model_registry table created by mysql-init/init.sql.
    managed=False: Django never creates, alters, or drops this table —
    training-job owns it entirely, via train.py's own INSERT statements.
    """

    hdfs_path = models.CharField(max_length=255)
    version = models.CharField(max_length=50)
    trained_on = models.CharField(max_length=50, null=True, blank=True)
    auc_score = models.FloatField(null=True, blank=True)
    accuracy = models.FloatField(null=True, blank=True)
    precision_score = models.FloatField(null=True, blank=True)
    recall_score = models.FloatField(null=True, blank=True)
    precision_purchase = models.FloatField(null=True, blank=True)
    recall_purchase = models.FloatField(null=True, blank=True)
    decision_threshold = models.FloatField(null=True, blank=True)
    is_active = models.BooleanField(default=False)
    created_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = "model_registry"
        ordering = ["created_at"]

    def __str__(self):
        return f"{self.version} ({'active' if self.is_active else 'inactive'})"


class Prediction(models.Model):
    """
    Maps onto the predictions table — written by streaming-job's
    write_predictions_partition(). Read-only from Django's side.
    """

    user_session = models.CharField(max_length=100)
    event_time = models.DateTimeField(null=True, blank=True)
    purchase_probability = models.FloatField(null=True, blank=True)
    predicted_label = models.BooleanField(null=True, blank=True)
    model_version = models.CharField(max_length=50, null=True, blank=True)
    scored_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = "predictions"
        ordering = ["-scored_at"]

    def __str__(self):
        return f"{self.user_session} -> {self.purchase_probability:.2f}"


class RunningMetric(models.Model):
    """
    Maps onto the running_metrics summary table. Read-only from Django.
    """

    metric_key = models.CharField(max_length=50, primary_key=True)
    metric_value = models.BigIntegerField(default=0)

    class Meta:
        managed = False
        db_table = "running_metrics"

    def __str__(self):
        return f"{self.metric_key}: {self.metric_value}"
