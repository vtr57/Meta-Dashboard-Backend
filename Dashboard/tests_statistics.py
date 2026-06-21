from datetime import date

from django.contrib.auth import get_user_model
from django.test import Client, TestCase

from Dashboard.models import AdAccount, Campaign, CampaignInsightDaily, DashboardUser
from Dashboard.services.statistics_utils import (
    descriptive_statistics,
    pearson_correlation,
    percent_change,
    safe_ratio,
    two_proportion_z_test,
)


User = get_user_model()


class StatisticsUtilsTests(TestCase):
    def test_safe_formulas_and_percent_change(self):
        self.assertEqual(safe_ratio(10, 0), None)
        self.assertEqual(safe_ratio(10, 100, 100), 10)
        self.assertEqual(percent_change(80, 100), -20)
        self.assertIsNone(percent_change(10, 0))

    def test_descriptive_statistics_classifies_stability(self):
        stats = descriptive_statistics([20, 21, 19, 20])
        self.assertEqual(stats['sample_size'], 4)
        self.assertEqual(stats['stability_label'], 'estável')
        self.assertLess(stats['coefficient_of_variation'], 0.25)

    def test_two_proportion_test_detects_relevant_difference(self):
        result = two_proportion_z_test(300, 10000, 220, 10000)
        self.assertTrue(result['available'])
        self.assertTrue(result['is_significant'])
        self.assertLess(result['p_value'], 0.05)

    def test_pearson_correlation_requires_variation(self):
        self.assertEqual(pearson_correlation([1, 2, 3], [2, 4, 6]), 1.0)
        self.assertIsNone(pearson_correlation([1, 1, 1], [2, 3, 4]))


class StatisticsAnalysisEndpointTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='statistics-user', password='Secret123!')
        self.client = Client()
        self.client.force_login(self.user)
        self.dashboard_user = DashboardUser.objects.create(
            user=self.user,
            id_meta_user='meta-statistics-user',
            long_access_token='token',
        )
        self.ad_account = AdAccount.objects.create(
            id_meta_ad_account='act_stats',
            name='Conta Estatística',
            id_dashboard_user=self.dashboard_user,
        )
        self.campaign_a = Campaign.objects.create(
            id_meta_campaign='cmp_stats_a',
            id_meta_ad_account=self.ad_account,
            name='Campanha A',
        )
        self.campaign_b = Campaign.objects.create(
            id_meta_campaign='cmp_stats_b',
            id_meta_ad_account=self.ad_account,
            name='Campanha B',
        )

        rows = [
            (self.campaign_a, date(2026, 1, 1), '15', 700, 500, 20, 40),
            (self.campaign_a, date(2026, 1, 2), '18', 800, 540, 22, 48),
            (self.campaign_a, date(2026, 1, 3), '20', 1000, 620, 30, 60),
            (self.campaign_a, date(2026, 1, 4), '22', 1100, 660, 32, 66),
            (self.campaign_b, date(2026, 1, 1), '12', 700, 480, 8, 24),
            (self.campaign_b, date(2026, 1, 2), '14', 800, 520, 9, 28),
            (self.campaign_b, date(2026, 1, 3), '16', 1000, 600, 10, 30),
            (self.campaign_b, date(2026, 1, 4), '18', 1100, 640, 11, 33),
        ]
        for campaign, created_at, spend, impressions, reach, results, clicks in rows:
            CampaignInsightDaily.objects.create(
                id_meta_campaign=campaign,
                created_at=created_at,
                gasto_diario=spend,
                impressao_diaria=impressions,
                alcance_diario=reach,
                quantidade_results_diaria=results,
                quantidade_clicks_diaria=clicks,
            )

    def test_analysis_returns_modular_payload_for_multiple_campaigns(self):
        response = self.client.get(
            '/api/statistics/analysis',
            {
                'ad_account_id': 'act_stats',
                'campaign_id': ['cmp_stats_a', 'cmp_stats_b'],
                'date_start': '2026-01-03',
                'date_end': '2026-01-04',
                'compare': 'true',
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['meta']['analysis_level'], 'campaign')
        self.assertEqual(payload['meta']['filters']['campaign_ids'], ['cmp_stats_a', 'cmp_stats_b'])
        self.assertEqual(payload['meta']['previous_date_start'], '2026-01-01')
        self.assertEqual(payload['meta']['previous_date_end'], '2026-01-02')
        self.assertTrue(payload['overview']['available'])
        overview = {item['metric']: item for item in payload['overview']['metrics']}
        self.assertEqual(overview['spend']['current_value'], 76.0)
        self.assertEqual(overview['results']['current_value'], 83)
        self.assertIsNotNone(overview['spend']['percent_change'])
        self.assertTrue(payload['stability']['available'])
        self.assertEqual({item['entity_id'] for item in payload['stability']['items']}, {'cmp_stats_a', 'cmp_stats_b'})
        self.assertTrue(payload['funnel']['available'])
        self.assertTrue(payload['ab_tests']['available'])
        self.assertTrue(payload['saturation']['available'])
        self.assertTrue(payload['trends']['available'])
        self.assertFalse(payload['segments']['available'])
        self.assertFalse(payload['cohorts']['available'])
        self.assertTrue(payload['executive_insights']['available'])

    def test_analysis_can_disable_previous_period_comparison(self):
        response = self.client.get(
            '/api/statistics/analysis',
            {
                'ad_account_id': 'act_stats',
                'date_start': '2026-01-03',
                'date_end': '2026-01-04',
                'compare': 'false',
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload['meta']['compare'])
        self.assertIsNone(payload['meta']['previous_date_start'])
        spend = next(item for item in payload['overview']['metrics'] if item['metric'] == 'spend')
        self.assertIsNone(spend['previous_value'])
        self.assertEqual(spend['direction'], 'sem_comparacao')

    def test_analysis_rejects_account_outside_user_scope(self):
        other_user = User.objects.create_user(username='statistics-other', password='Secret123!')
        other_dashboard_user = DashboardUser.objects.create(
            user=other_user,
            id_meta_user='meta-statistics-other',
            long_access_token='token',
        )
        AdAccount.objects.create(
            id_meta_ad_account='act_stats_other',
            name='Conta Externa',
            id_dashboard_user=other_dashboard_user,
        )

        response = self.client.get(
            '/api/statistics/analysis',
            {'ad_account_id': 'act_stats_other'},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'Ad account invalido para este usuario.')

    def test_analysis_requires_authentication(self):
        response = Client().get('/api/statistics/analysis')
        self.assertEqual(response.status_code, 403)
