import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('Dashboard', '0005_adaccount_shared_dashboard_users'),
    ]

    operations = [
        migrations.CreateModel(
            name='InstagramAccountInsightDaily',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('accounts_reached', models.PositiveBigIntegerField(default=0)),
                ('impressions', models.PositiveBigIntegerField(default=0)),
                ('profile_views', models.PositiveBigIntegerField(default=0)),
                ('accounts_engaged', models.PositiveBigIntegerField(default=0)),
                ('follower_count', models.PositiveBigIntegerField(blank=True, null=True)),
                ('follows_and_unfollows', models.IntegerField(default=0)),
                ('created_at', models.DateField(db_index=True)),
                (
                    'id_meta_instagram',
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name='daily_insights',
                        to='Dashboard.instagramaccount',
                    ),
                ),
            ],
            options={
                'indexes': [models.Index(fields=['id_meta_instagram', 'created_at'], name='Dashboard_i_id_meta_a895d5_idx')],
                'constraints': [
                    models.UniqueConstraint(
                        fields=('id_meta_instagram', 'created_at'),
                        name='uniq_instagram_account_insight_daily',
                    )
                ],
            },
        ),
    ]
