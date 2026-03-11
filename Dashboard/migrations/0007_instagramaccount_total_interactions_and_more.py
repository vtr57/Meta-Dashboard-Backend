from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('Dashboard', '0006_instagramaccountinsightdaily'),
    ]

    operations = [
        migrations.AddField(
            model_name='instagramaccount',
            name='total_interactions',
            field=models.PositiveBigIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='instagramaccountinsightdaily',
            name='total_interactions',
            field=models.PositiveBigIntegerField(default=0),
        ),
    ]
