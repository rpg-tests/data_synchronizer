from django.db import models


class EventLog(models.Model):
    event_date = models.DateField()
    is_success = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)


class ReservationLog(models.Model):
    last_sync_at = models.DateTimeField()

    PERIOD_DAILY = 'day'
    PERIOD_MONTHLY = 'month'
    PERIOD_YEARLY = 'year'
    PERIOD_CHOICES = (
        (PERIOD_DAILY, 'Daily'),
        (PERIOD_MONTHLY, 'Monthly'),
        (PERIOD_YEARLY, 'Yearly'),
    )
    period_type = models.CharField(choices=PERIOD_CHOICES)
    period_start = models.DateTimeField()
    period_end = models.DateTimeField()

    is_success = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
