from datetime import date

from django.contrib.auth import get_user_model
from django.test import Client, TestCase

from Dashboard.models import (
    Ad,
    AdAccount,
    AdInsightDaily,
    AdSet,
    Campaign,
    CampaignInsightDaily,
    DashboardUser,
)
from Dashboard.services.statistics_clustering_service import build_clustering_analysis
from Dashboard.services.statistics_service import build_correlations
from Dashboard.services.statistics_utils import (
    deterministic_kmeans,
    descriptive_statistics,
    pca_projection,
    pearson_correlation,
    percent_change,
    safe_ratio,
    standardize_matrix,
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

    def test_correlation_service_builds_symmetric_matrix_and_unavailable_sources(self):
        result = build_correlations(
            [
                {
                    'date': date(2026, 1, day),
                    'spend': day * 10,
                    'impressions': day * 100,
                    'reach': day * 80,
                    'clicks': day * 10,
                    'results': day * 2,
                }
                for day in range(1, 5)
            ]
        )

        self.assertTrue(result['available'])
        self.assertEqual(result['sample_size'], 4)
        self.assertEqual(len(result['metrics']), 10)
        self.assertEqual(len(result['matrix']), 10)
        matrix = {
            row['metric']: {cell['metric']: cell['value'] for cell in row['cells']}
            for row in result['matrix']
        }
        self.assertEqual(matrix['spend']['results'], 1.0)
        self.assertEqual(matrix['results']['spend'], matrix['spend']['results'])
        self.assertIsNone(matrix['cpc']['cpc'])
        self.assertEqual(
            {item['metric'] for item in result['unavailable_metrics']},
            {'delivery', 'budget', 'video_3s_rate', 'messaging_conversion_rate'},
        )

    def test_clustering_math_normalizes_groups_and_projects(self):
        normalized = standardize_matrix([[1, 10], [2, 20], [9, 90], [10, 100]])
        self.assertAlmostEqual(sum(row[0] for row in normalized['matrix']), 0)

        clustered = deterministic_kmeans(normalized['matrix'], 2)
        self.assertEqual(clustered['labels'][0], clustered['labels'][1])
        self.assertEqual(clustered['labels'][2], clustered['labels'][3])
        self.assertNotEqual(clustered['labels'][0], clustered['labels'][2])
        self.assertTrue(all(distance >= 0 for distance in clustered['distances']))

        pca = pca_projection(normalized['matrix'])
        self.assertTrue(pca['available'])
        self.assertEqual(len(pca['points']), 4)
        self.assertEqual(len(pca['points'][0]), 2)

    def test_clustering_service_handles_small_sample_and_cluster_limit(self):
        insufficient = build_clustering_analysis(
            rows=[
                {
                    'entity_id': str(index),
                    'entity_name': f'Campanha {index}',
                    'spend': 10 + index,
                    'impressions': 100 + index,
                    'reach': 80 + index,
                    'clicks': 10 + index,
                    'results': 2 + index,
                }
                for index in range(4)
            ],
            entity_type='campaign',
            requested_clusters=3,
        )
        self.assertFalse(insufficient['available'])
        self.assertIn('pelo menos 5', insufficient['message'])

        clustered = build_clustering_analysis(
            rows=[
                {
                    'entity_id': str(index),
                    'entity_name': f'Campanha {index}',
                    'spend': 10 + (index * 20),
                    'impressions': 100 + (index * 200),
                    'reach': 80 + (index * 130),
                    'clicks': 10 + (index * 15),
                    'results': 1 + (index * 3),
                }
                for index in range(6)
            ],
            entity_type='campaign',
            requested_clusters=5,
        )
        self.assertTrue(clustered['available'])
        self.assertEqual(clustered['clusters_count'], 2)
        self.assertTrue(any('reduzida para 2' in warning for warning in clustered['warnings']))
        self.assertTrue(any('foi ignorada' in warning for warning in clustered['warnings']))
        self.assertTrue(all(cluster['label'] for cluster in clustered['clusters']))
        self.assertTrue(all(cluster['suggested_action'] for cluster in clustered['clusters']))
        self.assertTrue(all(item['cluster_distance'] is not None for item in clustered['items']))


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

    def test_analysis_returns_correlation_matrix_for_full_daily_sample(self):
        response = self.client.get(
            '/api/statistics/analysis',
            {
                'ad_account_id': 'act_stats',
                'date_start': '2026-01-01',
                'date_end': '2026-01-04',
                'compare': 'false',
            },
        )

        self.assertEqual(response.status_code, 200)
        correlations = response.json()['correlations']
        self.assertTrue(correlations['available'])
        self.assertEqual(correlations['sample_size'], 4)
        self.assertEqual(len(correlations['metrics']), 10)
        self.assertEqual(len(correlations['matrix']), 10)
        spend_row = next(row for row in correlations['matrix'] if row['metric'] == 'spend')
        results_cell = next(cell for cell in spend_row['cells'] if cell['metric'] == 'results')
        self.assertIsNotNone(results_cell['value'])
        self.assertEqual(len(correlations['unavailable_metrics']), 4)

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

    def test_clustering_returns_campaign_groups_and_reduces_clusters(self):
        for index in range(3):
            campaign = Campaign.objects.create(
                id_meta_campaign=f'cmp_stats_extra_{index}',
                id_meta_ad_account=self.ad_account,
                name=f'Campanha Extra {index}',
            )
            CampaignInsightDaily.objects.create(
                id_meta_campaign=campaign,
                created_at=date(2026, 1, 4),
                gasto_diario=str(35 + (index * 25)),
                impressao_diaria=1200 + (index * 500),
                alcance_diario=800 + (index * 300),
                quantidade_results_diaria=8 + (index * 9),
                quantidade_clicks_diaria=45 + (index * 20),
            )

        response = self.client.get(
            '/api/statistics/clustering',
            {
                'ad_account_id': 'act_stats',
                'date_start': '2026-01-01',
                'date_end': '2026-01-04',
                'entity_type': 'campaign',
                'algorithm': 'kmeans',
                'clusters': '5',
                'normalize': 'true',
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['available'])
        self.assertEqual(payload['sample_size'], 5)
        self.assertEqual(payload['clusters_count'], 2)
        self.assertEqual(payload['meta']['entity_type'], 'campaign')
        self.assertTrue(payload['pca']['available'])
        self.assertTrue(payload['executive_insights']['available'])
        self.assertTrue(any('reduzida para 2' in warning for warning in payload['warnings']))

    def test_clustering_supports_ads_within_accessible_account(self):
        for index in range(5):
            adset = AdSet.objects.create(
                id_meta_adset=f'adset_cluster_{index}',
                id_meta_campaign=self.campaign_a,
                name=f'Conjunto Cluster {index}',
            )
            ad = Ad.objects.create(
                id_meta_ad=f'ad_cluster_{index}',
                id_meta_adset=adset,
                name=f'Anúncio Cluster {index}',
            )
            AdInsightDaily.objects.create(
                id_meta_ad=ad,
                created_at=date(2026, 1, 4),
                gasto_diario=str(10 + (index * 12)),
                impressao_diaria=300 + (index * 250),
                alcance_diario=220 + (index * 180),
                quantidade_results_diaria=2 + (index * 3),
                quantidade_clicks_diaria=15 + (index * 11),
            )

        response = self.client.get(
            '/api/statistics/clustering',
            {
                'ad_account_id': 'act_stats',
                'campaign_id': 'cmp_stats_a',
                'date_start': '2026-01-01',
                'date_end': '2026-01-04',
                'entity_type': 'ad',
                'clusters': '2',
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['available'])
        self.assertEqual(payload['sample_size'], 5)
        self.assertEqual({item['id'] for item in payload['items']}, {f'ad_cluster_{index}' for index in range(5)})

    def test_clustering_rejects_account_outside_user_scope(self):
        other_user = User.objects.create_user(username='clustering-other', password='Secret123!')
        other_dashboard_user = DashboardUser.objects.create(
            user=other_user,
            id_meta_user='meta-clustering-other',
            long_access_token='token',
        )
        AdAccount.objects.create(
            id_meta_ad_account='act_clustering_other',
            name='Conta Externa Cluster',
            id_dashboard_user=other_dashboard_user,
        )

        response = self.client.get(
            '/api/statistics/clustering',
            {
                'ad_account_id': 'act_clustering_other',
                'date_start': '2026-01-01',
                'date_end': '2026-01-04',
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'Ad account invalido para este usuario.')

    def test_clustering_lead_returns_explicit_unavailable_contract(self):
        response = self.client.get(
            '/api/statistics/clustering',
            {
                'ad_account_id': 'act_stats',
                'date_start': '2026-01-01',
                'date_end': '2026-01-04',
                'entity_type': 'lead',
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload['available'])
        self.assertIn('dados comerciais', payload['message'])

    def test_clustering_requires_authentication(self):
        response = Client().get('/api/statistics/clustering')
        self.assertEqual(response.status_code, 403)
