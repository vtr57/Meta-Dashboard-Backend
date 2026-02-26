from rest_framework import serializers

from Dashboard.models import AdAccount, Anotacoes, DashboardUser


class AnotacoesSerializer(serializers.ModelSerializer):
    id_meta_ad_account = serializers.SlugRelatedField(
        slug_field='id_meta_ad_account',
        queryset=AdAccount.objects.none(),
    )

    class Meta:
        model = Anotacoes
        fields = ['id', 'id_meta_ad_account', 'observacoes', 'data_criacao']
        read_only_fields = ['id', 'data_criacao']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        dashboard_user = self.context.get('dashboard_user')
        if isinstance(dashboard_user, DashboardUser):
            self.fields['id_meta_ad_account'].queryset = AdAccount.objects.filter(
                id_dashboard_user=dashboard_user
            ).order_by('id_meta_ad_account')

