from django.conf import settings
from django.db import models
from django.utils import timezone


class DashboardUser(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='dashboard_profile',
    )
    id_meta_user = models.CharField(max_length=64, unique=True, db_index=True)
    long_access_token = models.TextField()
    expired_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=['expired_at']),
        ]

    def __str__(self):
        return f'{self.user.username} ({self.id_meta_user})'

    def has_valid_long_token(self) -> bool:
        if not self.long_access_token:
            return False
        if self.expired_at is None:
            return True
        return self.expired_at > timezone.now()


class AdAccount(models.Model):
    id_meta_ad_account = models.CharField(max_length=64, unique=True, db_index=True)
    name = models.CharField(max_length=255)
    id_dashboard_user = models.ForeignKey(
        DashboardUser,
        on_delete=models.CASCADE,
        related_name='ad_accounts',
    )

    def __str__(self):
        return f'{self.name} ({self.id_meta_ad_account})'


class Campaign(models.Model):
    id_meta_campaign = models.CharField(max_length=64, unique=True, db_index=True)
    id_meta_ad_account = models.ForeignKey(
        AdAccount,
        on_delete=models.CASCADE,
        related_name='campaigns',
    )
    name = models.CharField(max_length=255)
    status = models.CharField(max_length=100, blank=True)
    created_time = models.DateTimeField(null=True, blank=True, db_index=True)
    effective_status = models.CharField(max_length=100, blank=True)

    def __str__(self):
        return f'{self.name} ({self.id_meta_campaign})'


class AdSet(models.Model):
    id_meta_adset = models.CharField(max_length=64, unique=True, db_index=True)
    id_meta_campaign = models.ForeignKey(
        Campaign,
        on_delete=models.CASCADE,
        related_name='adsets',
    )
    name = models.CharField(max_length=255)
    status = models.CharField(max_length=100, blank=True)
    created_time = models.DateTimeField(null=True, blank=True, db_index=True)
    effective_status = models.CharField(max_length=100, blank=True)

    def __str__(self):
        return f'{self.name} ({self.id_meta_adset})'


class Ad(models.Model):
    id_meta_ad = models.CharField(max_length=64, unique=True, db_index=True)
    id_meta_adset = models.ForeignKey(
        AdSet,
        on_delete=models.CASCADE,
        related_name='ads',
    )
    name = models.CharField(max_length=255)
    status = models.CharField(max_length=100, blank=True)
    created_time = models.DateTimeField(null=True, blank=True, db_index=True)
    effective_status = models.CharField(max_length=100, blank=True)

    def __str__(self):
        return f'{self.name} ({self.id_meta_ad})'


class InsightMetricsBase(models.Model):
    gasto_diario = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    impressao_diaria = models.PositiveBigIntegerField(default=0)
    alcance_diario = models.PositiveBigIntegerField(default=0)
    quantidade_results_diaria = models.PositiveBigIntegerField(default=0)
    ctr_medio = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    cpm_medio = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    cpc_medio = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    frequencia_media = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    quantidade_clicks_diaria = models.PositiveBigIntegerField(default=0)
    created_at = models.DateField(db_index=True)

    class Meta:
        abstract = True


class CampaignInsightDaily(InsightMetricsBase):
    id_meta_campaign = models.ForeignKey(
        Campaign,
        on_delete=models.CASCADE,
        related_name='daily_insights',
    )

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['id_meta_campaign', 'created_at'],
                name='uniq_campaign_insight_daily',
            ),
        ]
        indexes = [
            models.Index(fields=['id_meta_campaign', 'created_at']),
        ]


class AdSetInsightDaily(InsightMetricsBase):
    id_meta_adset = models.ForeignKey(
        AdSet,
        on_delete=models.CASCADE,
        related_name='daily_insights',
    )

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['id_meta_adset', 'created_at'],
                name='uniq_adset_insight_daily',
            ),
        ]
        indexes = [
            models.Index(fields=['id_meta_adset', 'created_at']),
        ]


class AdInsightDaily(InsightMetricsBase):
    id_meta_ad = models.ForeignKey(
        Ad,
        on_delete=models.CASCADE,
        related_name='daily_insights',
    )

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['id_meta_ad', 'created_at'],
                name='uniq_ad_insight_daily',
            ),
        ]
        indexes = [
            models.Index(fields=['id_meta_ad', 'created_at']),
        ]


class FacebookPage(models.Model):
    id_meta_page = models.CharField(max_length=64, unique=True, db_index=True)
    name = models.CharField(max_length=255)
    dashboard_user_id = models.ForeignKey(
        DashboardUser,
        on_delete=models.CASCADE,
        related_name='facebook_pages',
    )

    def __str__(self):
        return f'{self.name} ({self.id_meta_page})'


class InstagramAccount(models.Model):
    id_meta_instagram = models.CharField(max_length=64, unique=True, db_index=True)
    id_page = models.ForeignKey(
        FacebookPage,
        on_delete=models.CASCADE,
        related_name='instagram_accounts',
    )
    name = models.CharField(max_length=255)
    accounts_reached = models.PositiveBigIntegerField(null=True, blank=True)
    impressions = models.PositiveBigIntegerField(null=True, blank=True)
    profile_views = models.PositiveBigIntegerField(null=True, blank=True)
    accounts_engaged = models.PositiveBigIntegerField(null=True, blank=True)
    follower_count = models.PositiveBigIntegerField(null=True, blank=True)
    follows_and_unfollows = models.IntegerField(null=True, blank=True)

    def __str__(self):
        return f'{self.name} ({self.id_meta_instagram})'


class MediaInstagram(models.Model):
    id_meta_media = models.CharField(max_length=64, unique=True, db_index=True)
    id_meta_instagram = models.ForeignKey(
        InstagramAccount,
        on_delete=models.CASCADE,
        related_name='media_items',
    )
    caption = models.TextField(blank=True)
    media_type = models.CharField(max_length=50, blank=True)
    media_url = models.URLField(max_length=1000, blank=True)
    permalink = models.URLField(max_length=500, blank=True)
    timestamp = models.DateTimeField(null=True, blank=True, db_index=True)
    reach = models.PositiveBigIntegerField(null=True, blank=True)
    views = models.PositiveBigIntegerField(null=True, blank=True)
    likes = models.PositiveBigIntegerField(null=True, blank=True)
    comments = models.PositiveBigIntegerField(null=True, blank=True)
    saved = models.PositiveBigIntegerField(null=True, blank=True)
    shares = models.PositiveBigIntegerField(null=True, blank=True)
    plays = models.PositiveBigIntegerField(null=True, blank=True)
    watch_time = models.PositiveBigIntegerField(null=True, blank=True)
    avg_watch_time = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=['id_meta_instagram', 'timestamp']),
        ]

    def __str__(self):
        return self.id_meta_media


class SyncRun(models.Model):
    class Status(models.TextChoices):
        PENDING = 'pending', 'Pending'
        RUNNING = 'running', 'Running'
        SUCCESS = 'success', 'Success'
        FAILED = 'failed', 'Failed'

    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING, db_index=True)
    started_at = models.DateTimeField(auto_now_add=True, db_index=True)
    finished_at = models.DateTimeField(null=True, blank=True, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=['status', 'started_at']),
        ]


class SyncLog(models.Model):
    sync_run = models.ForeignKey(
        SyncRun,
        on_delete=models.CASCADE,
        related_name='logs',
    )
    entidade = models.CharField(max_length=100, db_index=True)
    mensagem = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=['sync_run', 'timestamp']),
            models.Index(fields=['entidade', 'timestamp']),
        ]
