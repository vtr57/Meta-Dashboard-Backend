from django.contrib import admin
from .models import (
    Ad,
    AdAccount,
    AdInsightDaily,
    AdSet,
    AdSetInsightDaily,
    Campaign,
    CampaignInsightDaily,
    DashboardUser,
    FacebookPage,
    InstagramAccount,
    MediaInstagram,
)

admin.site.register(DashboardUser)
admin.site.register(MediaInstagram)
admin.site.register(InstagramAccount)
admin.site.register(FacebookPage)
admin.site.register(AdInsightDaily)
admin.site.register(AdSetInsightDaily)
admin.site.register(CampaignInsightDaily)
admin.site.register(AdAccount)
admin.site.register(Campaign)
admin.site.register(AdSet)
admin.site.register(Ad)
