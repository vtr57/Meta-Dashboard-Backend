from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('Dashboard', '0004_anotacoes'),
    ]

    operations = [
        migrations.AddField(
            model_name='adaccount',
            name='shared_dashboard_users',
            field=models.ManyToManyField(blank=True, related_name='shared_meta_ad_accounts', to='Dashboard.dashboarduser'),
        ),
    ]
