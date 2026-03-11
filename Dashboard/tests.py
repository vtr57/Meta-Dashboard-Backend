import json
from datetime import date, datetime, timedelta, timezone
from unittest.mock import Mock, patch

import requests
from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.utils.dateparse import parse_date

from Dashboard.models import (
    Ad,
    AdAccount,
    AdInsightDaily,
    AdSet,
    AdSetInsightDaily,
    Anotacoes,
    Campaign,
    CampaignInsightDaily,
    DashboardUser,
    FacebookPage,
    InstagramAccount,
    InstagramAccountInsightDaily,
    MediaInstagram,
    SyncLog,
    SyncRun,
)
from Dashboard.services.meta_client import MetaClientError, MetaGraphClient
from Dashboard.services.meta_sync_orchestrator import MetaSyncOrchestrator


User = get_user_model()


class AuthSessionCsrfTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='alice', password='Secret123!')

    def test_login_logout_requires_csrf_and_uses_session(self):
        client = Client(enforce_csrf_checks=True)

        me_response = client.get('/auth/me/')
        self.assertEqual(me_response.status_code, 200)
        self.assertIn('csrftoken', client.cookies)

        no_csrf_login = client.post(
            '/auth/login/',
            data=json.dumps({'username': 'alice', 'password': 'Secret123!'}),
            content_type='application/json',
        )
        self.assertEqual(no_csrf_login.status_code, 403)

        csrf_token = client.cookies['csrftoken'].value
        login_response = client.post(
            '/auth/login/',
            data=json.dumps({'username': 'alice', 'password': 'Secret123!'}),
            content_type='application/json',
            HTTP_X_CSRFTOKEN=csrf_token,
        )
        self.assertEqual(login_response.status_code, 200)
        self.assertTrue(login_response.json()['authenticated'])

        me_after_login = client.get('/auth/me/')
        self.assertEqual(me_after_login.status_code, 200)
        self.assertTrue(me_after_login.json()['authenticated'])
        self.assertEqual(me_after_login.json()['user']['username'], 'alice')

        logout_csrf = client.cookies['csrftoken'].value
        logout_response = client.post('/auth/logout/', HTTP_X_CSRFTOKEN=logout_csrf)
        self.assertEqual(logout_response.status_code, 200)
        self.assertFalse(logout_response.json()['authenticated'])

        me_after_logout = client.get('/auth/me/')
        self.assertEqual(me_after_logout.status_code, 200)
        self.assertFalse(me_after_logout.json()['authenticated'])


class MetaConnectEndpointTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='meta-connect-user', password='Secret123!')
        self.client = Client()
        self.client.force_login(self.user)

    def test_meta_connect_endpoint_is_removed(self):
        response = self.client.post('/api/meta/connect', data=json.dumps({}), content_type='application/json')
        self.assertEqual(response.status_code, 404)


class MetaClientTests(TestCase):
    def setUp(self):
        self.sync_run = SyncRun.objects.create(status=SyncRun.Status.PENDING)

    def test_request_with_retry_uses_exponential_backoff(self):
        client = MetaGraphClient(
            access_token='token-123',
            sync_run=self.sync_run,
            request_pause_seconds=0,
            max_retries=3,
        )
        ok_response = Mock()
        ok_response.status_code = 200
        ok_response.json.return_value = {'ok': True}
        ok_response.text = '{"ok":true}'

        with patch.object(
            client.session,
            'request',
            side_effect=[requests.RequestException('temporary'), ok_response],
        ) as mocked_request, patch('Dashboard.services.meta_client.time.sleep') as mocked_sleep:
            payload = client.request_with_retry('GET', 'me')

        self.assertEqual(payload, {'ok': True})
        self.assertEqual(mocked_request.call_count, 2)
        mocked_sleep.assert_called_once_with(2.0)
        self.assertTrue(SyncLog.objects.filter(sync_run=self.sync_run, entidade='meta_graph').exists())

    def test_paginate_with_paging_next(self):
        client = MetaGraphClient(access_token='token-123', request_pause_seconds=0)
        page_1 = {
            'data': [{'id': '1'}],
            'paging': {'next': 'https://graph.facebook.com/v22.0/me/adaccounts?after=abc&access_token=token-123'},
        }
        page_2 = {'data': [{'id': '2'}]}

        with patch.object(client, 'request_with_retry', side_effect=[page_1, page_2]) as mocked_request:
            rows = list(client.paginate('me/adaccounts', params={'limit': 1}, entity='ad_accounts'))

        self.assertEqual(rows, [{'id': '1'}, {'id': '2'}])
        self.assertEqual(mocked_request.call_count, 2)
        self.assertEqual(mocked_request.call_args_list[1].kwargs['params'], {})

    def test_paginate_logs_error_with_page_context(self):
        client = MetaGraphClient(access_token='token-123', sync_run=self.sync_run, request_pause_seconds=0)
        with patch.object(client, 'request_with_retry', side_effect=MetaClientError('generic failure')):
            with self.assertRaises(MetaClientError):
                list(client.paginate('me/adaccounts', params={'limit': 1}, entity='ad_accounts'))

        self.assertTrue(
            SyncLog.objects.filter(
                sync_run=self.sync_run,
                entidade='ad_accounts',
                mensagem__icontains='Pagination error on page 1',
            ).exists()
        )

    def test_batch_request_chunks_and_normalizes_response(self):
        client = MetaGraphClient(access_token='token-123', request_pause_seconds=0, batch_size=2)
        chunk_1 = [
            {'code': 200, 'body': '{"ok":1}'},
            {'code': 400, 'body': '{"error":"bad"}'},
        ]
        chunk_2 = [{'code': 200, 'body': '{"ok":2}'}]

        calls = [
            {'method': 'GET', 'relative_url': 'x'},
            {'method': 'GET', 'relative_url': 'y'},
            {'method': 'GET', 'relative_url': 'z'},
        ]
        with patch.object(client, 'request_with_retry', side_effect=[chunk_1, chunk_2]) as mocked_request:
            output = client.batch_request(calls, entity='meta_batch', batch_size=2)

        self.assertEqual(len(output), 3)
        self.assertEqual([row['status_code'] for row in output], [200, 400, 200])
        self.assertEqual(mocked_request.call_count, 2)


class InsightAggregationTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='bob', password='Secret123!')
        self.dashboard_user = DashboardUser.objects.create(
            user=self.user,
            id_meta_user='meta-user-1',
            long_access_token='token',
        )
        self.ad_account = AdAccount.objects.create(
            id_meta_ad_account='act_1',
            name='Conta 1',
            id_dashboard_user=self.dashboard_user,
        )
        self.campaign = Campaign.objects.create(
            id_meta_campaign='cmp_1',
            id_meta_ad_account=self.ad_account,
            name='Campanha 1',
        )
        self.adset = AdSet.objects.create(
            id_meta_adset='ads_1',
            id_meta_campaign=self.campaign,
            name='AdSet 1',
        )
        self.ad = Ad.objects.create(
            id_meta_ad='ad_1',
            id_meta_adset=self.adset,
            name='Ad 1',
        )
        self.sync_run = SyncRun.objects.create(status=SyncRun.Status.PENDING)

    def test_sync_ad_insights_aggregates_into_adset_and_campaign(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def paginate(self, path_or_url, *, params=None, entity='meta_graph', page_limit=None):
                self.calls.append({'path_or_url': path_or_url, 'params': params or {}, 'entity': entity})
                if path_or_url != 'act_1/insights':
                    return
                yield {
                    'ad_id': 'ad_1',
                    'date_start': '2026-01-01',
                    'date_stop': '2026-01-01',
                    'spend': '10',
                    'impressions': '100',
                    'reach': '50',
                    'clicks': '20',
                    'results': [
                        {
                            'indicator': 'actions:onsite_conversion.messaging_conversation_started_7d',
                            'values': [{'value': '7', 'attribution_windows': ['default']}],
                        }
                    ],
                    'ctr': '',
                    'cpm': '',
                    'cpc': '',
                    'frequency': '',
                }
                yield {
                    'ad_id': 'ad_1',
                    'date_start': '2026-01-02',
                    'date_stop': '2026-01-02',
                    'spend': '5',
                    'impressions': '50',
                    'reach': '25',
                    'clicks': '5',
                    'results': [
                        {
                            'indicator': 'actions:onsite_conversion.messaging_conversation_started_7d',
                            'values': [{'value': '2', 'attribution_windows': ['default']}],
                        }
                    ],
                    'ctr': '',
                    'cpm': '',
                    'cpc': '',
                    'frequency': '',
                }

        orchestrator = MetaSyncOrchestrator(sync_run_id=self.sync_run.id, dashboard_user_id=self.dashboard_user.id)
        orchestrator.dashboard_user = self.dashboard_user
        fake_client = FakeClient()
        orchestrator.client = fake_client

        result = orchestrator.sync_ad_insights(since=date(2026, 1, 1), until=date(2026, 1, 2))
        self.assertEqual(result['ad_insight_upserts'], 2)
        self.assertEqual(result['adset_insight_upserts'], 2)
        self.assertEqual(result['campaign_insight_upserts'], 2)
        self.assertTrue(any(call['path_or_url'] == 'act_1/insights' for call in fake_client.calls))
        first_call = fake_client.calls[0]
        self.assertEqual(first_call['params'].get('level'), 'ad')
        self.assertIn('results', first_call['params'].get('fields', ''))

        self.assertEqual(AdInsightDaily.objects.count(), 2)
        self.assertEqual(AdSetInsightDaily.objects.count(), 2)
        self.assertEqual(CampaignInsightDaily.objects.count(), 2)

        day_1 = CampaignInsightDaily.objects.get(
            id_meta_campaign=self.campaign,
            created_at=date(2026, 1, 1),
        )
        self.assertAlmostEqual(float(day_1.gasto_diario), 10.0, places=4)
        self.assertEqual(day_1.quantidade_results_diaria, 7)
        self.assertAlmostEqual(float(day_1.ctr_medio), 20.0, places=4)
        self.assertAlmostEqual(float(day_1.cpm_medio), 100.0, places=4)
        self.assertAlmostEqual(float(day_1.cpc_medio), 0.5, places=4)
        self.assertAlmostEqual(float(day_1.frequencia_media), 2.0, places=4)


class MetaDashboardEndpointsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='carol', password='Secret123!')
        self.client = Client()
        self.client.force_login(self.user)

        self.dashboard_user = DashboardUser.objects.create(
            user=self.user,
            id_meta_user='meta-user-2',
            long_access_token='token',
        )
        self.ad_account = AdAccount.objects.create(
            id_meta_ad_account='act_200',
            name='Conta 200',
            id_dashboard_user=self.dashboard_user,
        )
        self.campaign = Campaign.objects.create(
            id_meta_campaign='cmp_200',
            id_meta_ad_account=self.ad_account,
            name='Campanha 200',
        )
        self.adset = AdSet.objects.create(
            id_meta_adset='ads_200',
            id_meta_campaign=self.campaign,
            name='AdSet 200',
        )
        self.ad = Ad.objects.create(
            id_meta_ad='ad_200',
            id_meta_adset=self.adset,
            name='Ad 200',
            effective_status='ACTIVE',
        )
        self.ad_secondary = Ad.objects.create(
            id_meta_ad='ad_201',
            id_meta_adset=self.adset,
            name='Ad 201',
            effective_status='ACTIVE',
        )
        self.ad_inactive = Ad.objects.create(
            id_meta_ad='ad_202',
            id_meta_adset=self.adset,
            name='Ad 202',
            effective_status='PAUSED',
        )

        CampaignInsightDaily.objects.create(
            id_meta_campaign=self.campaign,
            created_at=date(2026, 1, 1),
            gasto_diario='10',
            impressao_diaria=100,
            alcance_diario=50,
            quantidade_results_diaria=5,
            quantidade_clicks_diaria=20,
        )
        CampaignInsightDaily.objects.create(
            id_meta_campaign=self.campaign,
            created_at=date(2026, 1, 2),
            gasto_diario='20',
            impressao_diaria=200,
            alcance_diario=100,
            quantidade_results_diaria=3,
            quantidade_clicks_diaria=10,
        )
        AdInsightDaily.objects.create(
            id_meta_ad=self.ad,
            created_at=date(2026, 1, 1),
            gasto_diario='3',
            quantidade_results_diaria=1,
        )
        AdInsightDaily.objects.create(
            id_meta_ad=self.ad,
            created_at=date(2026, 1, 2),
            gasto_diario='7',
            quantidade_results_diaria=3,
        )
        AdInsightDaily.objects.create(
            id_meta_ad=self.ad_secondary,
            created_at=date(2026, 1, 1),
            gasto_diario='2',
            quantidade_results_diaria=0,
        )
        AdInsightDaily.objects.create(
            id_meta_ad=self.ad_secondary,
            created_at=date(2026, 1, 2),
            gasto_diario='5',
            quantidade_results_diaria=0,
        )
        AdInsightDaily.objects.create(
            id_meta_ad=self.ad_inactive,
            created_at=date(2026, 1, 1),
            gasto_diario='99',
            quantidade_results_diaria=99,
        )

    def test_meta_filters_returns_account_hierarchy(self):
        response = self.client.get('/api/meta/filters')
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload['ad_accounts']), 1)
        self.assertEqual(payload['ad_accounts'][0]['id_meta_ad_account'], 'act_200')
        self.assertEqual(payload['campaigns'][0]['id_meta_campaign'], 'cmp_200')
        self.assertEqual(payload['adsets'][0]['id_meta_adset'], 'ads_200')
        self.assertEqual(payload['ads'][0]['id_meta_ad'], 'ad_200')

    def test_meta_timeseries_and_kpis(self):
        params = {
            'ad_account_id': 'act_200',
            'date_start': '2026-01-01',
            'date_end': '2026-01-02',
        }
        timeseries_response = self.client.get('/api/meta/timeseries', params)
        self.assertEqual(timeseries_response.status_code, 200)
        series = timeseries_response.json()['series']
        self.assertEqual(len(series), 2)
        self.assertEqual(series[0]['impressions'], 100)
        self.assertEqual(series[0]['results'], 5)
        self.assertEqual(series[1]['clicks'], 10)

        kpi_response = self.client.get('/api/meta/kpis', params)
        self.assertEqual(kpi_response.status_code, 200)
        kpis = kpi_response.json()['kpis']
        self.assertAlmostEqual(kpis['gasto_total'], 30.0, places=4)
        self.assertEqual(kpis['impressao_total'], 300)
        self.assertEqual(kpis['alcance_total'], 150)
        self.assertEqual(kpis['results_total'], 8)
        self.assertAlmostEqual(kpis['ctr_medio'], 10.0, places=4)
        self.assertAlmostEqual(kpis['cpm_medio'], 100.0, places=4)
        self.assertAlmostEqual(kpis['cpc_medio'], 1.0, places=4)
        self.assertAlmostEqual(kpis['frequencia_media'], 2.0, places=4)

    def test_meta_specific_insights_returns_only_active_ads_and_daily_results(self):
        params = {
            'ad_account_id': 'act_200',
            'date_start': '2026-01-01',
            'date_end': '2026-01-02',
            'ad_id': 'ad_200',
        }
        response = self.client.get('/api/meta/specific-insights', params)
        self.assertEqual(response.status_code, 200)

        payload = response.json()
        self.assertEqual(payload['level'], 'ad_account')
        self.assertEqual(payload['filters']['ad_id'], '')

        timeseries_daily = payload['timeseries_daily']
        self.assertEqual(
            timeseries_daily,
            [
                {'date': '2026-01-01', 'spend': 5.0, 'results': 1},
                {'date': '2026-01-02', 'spend': 12.0, 'results': 3},
            ],
        )

        timeseries_by_ad = payload['timeseries_by_ad']
        self.assertEqual(
            timeseries_by_ad,
            [
                {
                    'ad_id': 'ad_200',
                    'ad_name': 'Ad 200',
                    'points': [
                        {'date': '2026-01-01', 'spend': 3.0, 'results': 1},
                        {'date': '2026-01-02', 'spend': 7.0, 'results': 3},
                    ],
                },
                {
                    'ad_id': 'ad_201',
                    'ad_name': 'Ad 201',
                    'points': [
                        {'date': '2026-01-01', 'spend': 2.0, 'results': 0},
                        {'date': '2026-01-02', 'spend': 5.0, 'results': 0},
                    ],
                },
            ],
        )

        rows_by_ad = payload['rows_by_ad']
        self.assertEqual(len(rows_by_ad), 2)
        self.assertEqual(rows_by_ad[0]['ad_id'], 'ad_200')
        self.assertEqual(rows_by_ad[0]['ad_name'], 'Ad 200')
        self.assertEqual(rows_by_ad[0]['results'], 4)
        self.assertEqual(rows_by_ad[0]['spend'], 10.0)
        self.assertEqual(rows_by_ad[0]['cpr'], 2.5)
        self.assertEqual(rows_by_ad[1]['ad_id'], 'ad_201')
        self.assertEqual(rows_by_ad[1]['results'], 0)
        self.assertEqual(rows_by_ad[1]['spend'], 7.0)
        self.assertIsNone(rows_by_ad[1]['cpr'])
        self.assertTrue(all(row['ad_id'] != 'ad_202' for row in rows_by_ad))

    def test_shared_ad_account_is_visible_to_second_user(self):
        other_user = User.objects.create_user(username='carol-shared', password='Secret123!')
        other_dashboard_user = DashboardUser.objects.create(
            user=other_user,
            id_meta_user='meta-user-shared',
            long_access_token='token',
        )
        self.ad_account.shared_dashboard_users.add(other_dashboard_user)

        other_client = Client()
        other_client.force_login(other_user)

        filters_response = other_client.get('/api/meta/filters')
        self.assertEqual(filters_response.status_code, 200)
        self.assertEqual(filters_response.json()['ad_accounts'][0]['id_meta_ad_account'], 'act_200')

        timeseries_response = other_client.get(
            '/api/meta/timeseries',
            {
                'ad_account_id': 'act_200',
                'date_start': '2026-01-01',
                'date_end': '2026-01-02',
            },
        )
        self.assertEqual(timeseries_response.status_code, 200)
        self.assertEqual(len(timeseries_response.json()['series']), 2)


class MetaAnotacoesEndpointsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='notes-user', password='Secret123!')
        self.client = Client()
        self.client.force_login(self.user)
        self.dashboard_user = DashboardUser.objects.create(
            user=self.user,
            id_meta_user='meta-user-notes',
            long_access_token='token',
        )
        self.ad_account = AdAccount.objects.create(
            id_meta_ad_account='act_notes_1',
            name='Conta Notes',
            id_dashboard_user=self.dashboard_user,
        )

        self.other_user = User.objects.create_user(username='notes-other', password='Secret123!')
        self.other_dashboard_user = DashboardUser.objects.create(
            user=self.other_user,
            id_meta_user='meta-user-notes-other',
            long_access_token='token',
        )
        self.other_ad_account = AdAccount.objects.create(
            id_meta_ad_account='act_notes_other',
            name='Conta Other',
            id_dashboard_user=self.other_dashboard_user,
        )

    def test_create_and_list_anotacoes_for_selected_account(self):
        create_response = self.client.post(
            '/api/meta/anotacoes',
            data=json.dumps(
                {
                    'id_meta_ad_account': self.ad_account.id_meta_ad_account,
                    'observacoes': 'Primeira observacao da conta.',
                }
            ),
            content_type='application/json',
        )
        self.assertEqual(create_response.status_code, 201)
        created = create_response.json()['anotacao']
        self.assertEqual(created['id_meta_ad_account'], self.ad_account.id_meta_ad_account)
        self.assertEqual(created['observacoes'], 'Primeira observacao da conta.')
        self.assertTrue(created['data_criacao'])

        Anotacoes.objects.create(
            id_meta_ad_account=self.other_ad_account,
            observacoes='Observacao de outro usuario.',
        )

        list_response = self.client.get('/api/meta/anotacoes', {'ad_account_id': self.ad_account.id_meta_ad_account})
        self.assertEqual(list_response.status_code, 200)
        rows = list_response.json()['anotacoes']
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['observacoes'], 'Primeira observacao da conta.')

    def test_create_anotacao_rejects_other_users_ad_account(self):
        response = self.client.post(
            '/api/meta/anotacoes',
            data=json.dumps(
                {
                    'id_meta_ad_account': self.other_ad_account.id_meta_ad_account,
                    'observacoes': 'Tentativa invalida.',
                }
            ),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn('id_meta_ad_account', response.json())
        self.assertEqual(Anotacoes.objects.filter(observacoes='Tentativa invalida.').count(), 0)

    def test_shared_ad_account_accepts_anotacao_from_second_user(self):
        self.ad_account.shared_dashboard_users.add(self.other_dashboard_user)

        other_client = Client()
        other_client.force_login(self.other_user)

        response = other_client.post(
            '/api/meta/anotacoes',
            data=json.dumps(
                {
                    'id_meta_ad_account': self.ad_account.id_meta_ad_account,
                    'observacoes': 'Observacao compartilhada.',
                }
            ),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, 201)
        self.assertEqual(
            response.json()['anotacao']['id_meta_ad_account'],
            self.ad_account.id_meta_ad_account,
        )

    def test_delete_anotacao_for_current_user(self):
        anotacao = Anotacoes.objects.create(
            id_meta_ad_account=self.ad_account,
            observacoes='Anotacao para excluir.',
        )
        response = self.client.delete(f'/api/meta/anotacoes/{anotacao.id}')
        self.assertEqual(response.status_code, 204)
        self.assertFalse(Anotacoes.objects.filter(id=anotacao.id).exists())

    def test_delete_anotacao_from_other_user_returns_404(self):
        other_note = Anotacoes.objects.create(
            id_meta_ad_account=self.other_ad_account,
            observacoes='Nao pode excluir.',
        )
        response = self.client.delete(f'/api/meta/anotacoes/{other_note.id}')
        self.assertEqual(response.status_code, 404)
        self.assertTrue(Anotacoes.objects.filter(id=other_note.id).exists())


class MetaSyncStartScopeEndpointsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='erin', password='Secret123!')
        self.client = Client()
        self.client.force_login(self.user)
        self.dashboard_user = DashboardUser.objects.create(
            user=self.user,
            id_meta_user='meta-user-sync-scope',
            long_access_token='token',
        )

    @patch('Dashboard.api_views.threading.Thread')
    def test_meta_sync_start_meta_endpoint(self, mocked_thread):
        mocked_thread.return_value = Mock()

        response = self.client.post('/api/meta/sync/start/meta')
        self.assertEqual(response.status_code, 202)
        payload = response.json()
        self.assertEqual(payload['sync_scope'], 'meta')

        args = mocked_thread.call_args.kwargs['args']
        self.assertEqual(args[1], self.dashboard_user.id)
        self.assertEqual(args[2], 'meta')
        self.assertIsNone(args[3])

    @patch('Dashboard.api_views.threading.Thread')
    def test_meta_sync_start_instagram_endpoint(self, mocked_thread):
        mocked_thread.return_value = Mock()

        response = self.client.post('/api/meta/sync/start/instagram')
        self.assertEqual(response.status_code, 202)
        payload = response.json()
        self.assertEqual(payload['sync_scope'], 'instagram')

        args = mocked_thread.call_args.kwargs['args']
        self.assertEqual(args[1], self.dashboard_user.id)
        self.assertEqual(args[2], 'instagram')
        self.assertIsNone(args[3])

    @patch('Dashboard.api_views.threading.Thread')
    def test_meta_sync_start_insights_7d_endpoint(self, mocked_thread):
        mocked_thread.return_value = Mock()

        response = self.client.post('/api/meta/sync/start/insights-7d')
        self.assertEqual(response.status_code, 202)
        payload = response.json()
        self.assertEqual(payload['sync_scope'], 'all')
        self.assertEqual(payload['insights_days_override'], 7)

        args = mocked_thread.call_args.kwargs['args']
        self.assertEqual(args[1], self.dashboard_user.id)
        self.assertEqual(args[2], 'all')
        self.assertEqual(args[3], 7)

    @patch('Dashboard.api_views.threading.Thread')
    def test_meta_sync_start_insights_1d_endpoint(self, mocked_thread):
        mocked_thread.return_value = Mock()

        response = self.client.post('/api/meta/sync/start/insights-1d')
        self.assertEqual(response.status_code, 202)
        payload = response.json()
        self.assertEqual(payload['sync_scope'], 'meta')
        self.assertEqual(payload['insights_days_override'], 1)

        args = mocked_thread.call_args.kwargs['args']
        self.assertEqual(args[1], self.dashboard_user.id)
        self.assertEqual(args[2], 'meta')
        self.assertEqual(args[3], 1)


class MetaBatchEntityExtractionTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='dave', password='Secret123!')
        self.dashboard_user = DashboardUser.objects.create(
            user=self.user,
            id_meta_user='meta-user-batch',
            long_access_token='token',
        )
        self.ad_account = AdAccount.objects.create(
            id_meta_ad_account='act_900',
            name='Conta 900',
            id_dashboard_user=self.dashboard_user,
        )
        self.sync_run = SyncRun.objects.create(status=SyncRun.Status.PENDING)

    def test_sync_campaign_adset_and_ad_use_batch(self):
        class FakeClient:
            graph_version = 'v24.0'

            def __init__(self):
                self.entities = []
                self.batch_sizes = []

            def batch_request(self, calls, *, entity='meta_batch', batch_size=None, include_headers=False):
                self.entities.append(entity)
                self.batch_sizes.append(batch_size)
                output = []
                for call in calls:
                    relative_url = str(call.get('relative_url') or '')
                    if '/campaigns' in relative_url:
                        body = {
                            'data': [
                                {
                                    'id': 'cmp_batch_1',
                                    'name': 'Campaign Batch',
                                    'status': 'ACTIVE',
                                    'created_time': '2026-01-01T00:00:00+0000',
                                    'effective_status': 'ACTIVE',
                                }
                            ]
                        }
                    elif '/adsets' in relative_url:
                        body = {
                            'data': [
                                {
                                    'id': 'ads_batch_1',
                                    'campaign_id': 'cmp_batch_1',
                                    'name': 'AdSet Batch',
                                    'status': 'ACTIVE',
                                    'created_time': '2026-01-01T00:00:00+0000',
                                    'effective_status': 'ACTIVE',
                                }
                            ]
                        }
                    elif '/ads' in relative_url and '/insights' not in relative_url:
                        body = {
                            'data': [
                                {
                                    'id': 'ad_batch_1',
                                    'adset_id': 'ads_batch_1',
                                    'name': 'Ad Batch',
                                    'status': 'ACTIVE',
                                    'created_time': '2026-01-01T00:00:00+0000',
                                    'effective_status': 'ACTIVE',
                                }
                            ]
                        }
                    else:
                        body = {'data': []}
                    output.append({'status_code': 200, 'headers': [], 'body': body, 'body_raw': ''})
                return output

        orchestrator = MetaSyncOrchestrator(sync_run_id=self.sync_run.id, dashboard_user_id=self.dashboard_user.id)
        orchestrator.dashboard_user = self.dashboard_user
        fake_client = FakeClient()
        orchestrator.client = fake_client

        campaigns_result = orchestrator.sync_campaigns()
        adsets_result = orchestrator.sync_adsets()
        ads_result = orchestrator.sync_ads()

        self.assertEqual(campaigns_result['campaigns_upserted'], 1)
        self.assertEqual(adsets_result['adsets_upserted'], 1)
        self.assertEqual(ads_result['ads_upserted'], 1)

        self.assertEqual(fake_client.entities, ['campaigns_batch', 'adsets_batch', 'ads_batch'])
        self.assertEqual(fake_client.batch_sizes, [50, 50, 50])

        self.assertTrue(Campaign.objects.filter(id_meta_campaign='cmp_batch_1').exists())
        self.assertTrue(AdSet.objects.filter(id_meta_adset='ads_batch_1').exists())
        self.assertTrue(Ad.objects.filter(id_meta_ad='ad_batch_1').exists())

    def test_sync_ad_accounts_preserves_owner_and_grants_shared_access(self):
        other_user = User.objects.create_user(username='batch-shared', password='Secret123!')
        other_dashboard_user = DashboardUser.objects.create(
            user=other_user,
            id_meta_user='meta-user-batch-shared',
            long_access_token='token',
        )

        class FakeClient:
            def paginate(self, *args, **kwargs):
                return iter([{'id': 'act_900', 'name': 'Conta 900 Atualizada'}])

        orchestrator = MetaSyncOrchestrator(sync_run_id=self.sync_run.id, dashboard_user_id=other_dashboard_user.id)
        orchestrator.dashboard_user = other_dashboard_user
        orchestrator.client = FakeClient()

        result = orchestrator.sync_ad_accounts()

        self.assertEqual(result['ad_accounts_upserted'], 1)
        self.ad_account.refresh_from_db()
        self.assertEqual(self.ad_account.id_dashboard_user, self.dashboard_user)
        self.assertTrue(self.ad_account.shared_dashboard_users.filter(id=other_dashboard_user.id).exists())


class MetaSyncOrchestratorPathTests(TestCase):
    def test_ad_account_edge_path_does_not_duplicate_act_prefix(self):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1)
        self.assertEqual(
            orchestrator._ad_account_edge_path('act_356273767805669', 'ads'),
            'act_356273767805669/ads',
        )
        self.assertEqual(
            orchestrator._ad_account_edge_path('356273767805669', 'ads'),
            'act_356273767805669/ads',
        )

    def test_iter_month_chunks_quarterly(self):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1)
        chunks = list(orchestrator._iter_month_chunks(date(2026, 1, 1), date(2026, 9, 15), chunk_months=3))
        self.assertEqual(
            chunks,
            [
                (date(2026, 1, 1), date(2026, 3, 31)),
                (date(2026, 4, 1), date(2026, 6, 30)),
                (date(2026, 7, 1), date(2026, 9, 15)),
            ],
        )

    @patch('Dashboard.services.meta_sync_orchestrator.timezone.localdate', return_value=date(2026, 2, 23))
    def test_build_date_window_with_insights_days_override(self, _mocked_today):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1, insights_days_override=7)
        since, until = orchestrator._build_date_window()
        self.assertEqual(since, date(2026, 2, 16))
        self.assertEqual(until, date(2026, 2, 23))

    @patch('Dashboard.services.meta_sync_orchestrator.timezone.localdate', return_value=date(2026, 2, 20))
    def test_fetch_instagram_account_insights_clamps_since_to_two_years(self, _mocked_today):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1)
        orchestrator.client = Mock()
        orchestrator.client.request_with_retry.return_value = {'data': []}

        orchestrator._fetch_instagram_account_insights(
            ig_id='17841455724736396',
            since=date(2023, 1, 1),
            until=date(2026, 2, 20),
        )

        first_call_params = orchestrator.client.request_with_retry.call_args_list[0].kwargs['params']
        self.assertEqual(first_call_params['since'], '2024-02-22')
        self.assertTrue(orchestrator.client.request_with_retry.call_count > 1)
        for call in orchestrator.client.request_with_retry.call_args_list:
            params = call.kwargs['params']
            since_value = parse_date(params['since'])
            until_value = parse_date(params['until'])
            self.assertIsNotNone(since_value)
            self.assertIsNotNone(until_value)
            self.assertLessEqual((until_value - since_value).days, 29)

    @patch('Dashboard.services.meta_sync_orchestrator.timezone.localdate', return_value=date(2026, 2, 20))
    def test_fetch_instagram_account_insights_uses_metric_type_total_value(self, _mocked_today):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1)
        orchestrator.client = Mock()
        orchestrator.client.request_with_retry.return_value = {'data': []}

        orchestrator._fetch_instagram_account_insights(
            ig_id='17841455724736396',
            since=date(2024, 6, 1),
            until=date(2026, 2, 20),
        )

        all_params = [call.kwargs['params'] for call in orchestrator.client.request_with_retry.call_args_list]
        self.assertTrue(any(params.get('metric_type') == 'total_value' for params in all_params))

    @patch('Dashboard.services.meta_sync_orchestrator.timezone.localdate', return_value=date(2026, 2, 20))
    def test_fetch_instagram_account_insights_limits_follower_count_to_last_30_days_excluding_today(
        self, _mocked_today
    ):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1)
        orchestrator.client = Mock()
        orchestrator.client.request_with_retry.return_value = {'data': []}

        orchestrator._fetch_instagram_account_insights(
            ig_id='17841455724736396',
            since=date(2024, 6, 1),
            until=date(2026, 2, 20),
        )

        all_params = [call.kwargs['params'] for call in orchestrator.client.request_with_retry.call_args_list]
        follower_params = [params for params in all_params if params.get('metric') == 'follower_count']
        self.assertEqual(len(follower_params), 1)
        self.assertEqual(follower_params[0]['since'], '2026-01-22')
        self.assertEqual(follower_params[0]['until'], '2026-02-18')

        grouped_metrics = [str(params.get('metric') or '') for params in all_params if ',' in str(params.get('metric') or '')]
        self.assertTrue(all('follower_count' not in metrics for metrics in grouped_metrics))

    @patch('Dashboard.services.meta_sync_orchestrator.timezone.localdate', return_value=date(2026, 2, 20))
    def test_fetch_instagram_account_insights_skips_when_window_is_outside_limit(self, _mocked_today):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1)
        orchestrator.client = Mock()

        payload = orchestrator._fetch_instagram_account_insights(
            ig_id='17841455724736396',
            since=date(2020, 1, 1),
            until=date(2023, 12, 31),
        )

        self.assertEqual(payload, {'data': []})
        orchestrator.client.request_with_retry.assert_not_called()

    def test_extract_results_value_reads_nested_values_list(self):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1)
        value = orchestrator._extract_results_value(
            [
                {
                    'indicator': 'actions:onsite_conversion.messaging_conversation_started_7d',
                    'values': [{'value': '3', 'attribution_windows': ['default']}],
                },
                {
                    'indicator': 'actions:onsite_conversion.messaging_conversation_started_7d',
                },
                {
                    'indicator': 'actions:onsite_conversion.messaging_conversation_started_7d',
                    'values': [{'value': '2'}],
                },
            ]
        )
        self.assertEqual(value, 5)

    def test_extract_batch_error_message_prefers_meta_error_message(self):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1)
        message = orchestrator._extract_batch_error_message(
            {
                'status_code': 400,
                'body': {'error': {'message': 'Unsupported get request'}},
                'body_raw': '{"error":{"message":"fallback"}}',
            }
        )
        self.assertEqual(message, 'Unsupported get request')

    def test_parse_instagram_account_insights_maps_reach_and_views(self):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1)
        payload = {
            'data': [
                {'name': 'reach', 'values': [{'value': '120'}]},
                {'name': 'views', 'values': [{'value': '340'}]},
                {'name': 'profile_views', 'values': [{'value': '22'}]},
                {'name': 'accounts_engaged', 'values': [{'value': '18'}]},
                {'name': 'follower_count', 'values': [{'value': '777'}]},
                {'name': 'follows_and_unfollows', 'values': [{'value': {'count': 5}}]},
            ]
        }

        updates = orchestrator._parse_instagram_account_insights(payload)
        self.assertEqual(updates['accounts_reached'], 120)
        self.assertEqual(updates['impressions'], 340)
        self.assertEqual(updates['profile_views'], 22)
        self.assertEqual(updates['accounts_engaged'], 18)
        self.assertEqual(updates['follower_count'], 777)
        self.assertEqual(updates['follows_and_unfollows'], 5)

    def test_media_metrics_for_type_uses_supported_metrics(self):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1)
        reel_metrics = orchestrator._media_metrics_for_type('REEL')
        video_metrics = orchestrator._media_metrics_for_type('VIDEO')

        self.assertIn('ig_reels_video_view_total_time', reel_metrics)
        self.assertNotIn('total_watch_time', reel_metrics)
        self.assertIn('views', video_metrics)
        self.assertNotIn('video_views', video_metrics)

    def test_parse_media_insights_maps_video_views_and_reels_watch_time(self):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1)
        updates = orchestrator._parse_media_insights(
            [
                {'name': 'video_views', 'values': [{'value': '91'}]},
                {'name': 'ig_reels_video_view_total_time', 'values': [{'value': '456'}]},
                {'name': 'ig_reels_avg_watch_time', 'values': [{'value': '12.7'}]},
            ]
        )

        self.assertEqual(updates['views'], 91)
        self.assertEqual(updates['watch_time'], 456)
        self.assertAlmostEqual(float(updates['avg_watch_time']), 12.7, places=4)

    def test_parse_instagram_account_daily_insights_maps_per_day(self):
        orchestrator = MetaSyncOrchestrator(sync_run_id=1, dashboard_user_id=1)
        payload = {
            'data': [
                {
                    'name': 'views',
                    'values': [
                        {'value': '120', 'end_time': '2026-02-01T07:00:00+0000'},
                        {'value': '140', 'end_time': '2026-02-02T07:00:00+0000'},
                    ],
                },
                {
                    'name': 'accounts_engaged',
                    'values': [
                        {'value': '20', 'end_time': '2026-02-01T07:00:00+0000'},
                        {'value': '26', 'end_time': '2026-02-02T07:00:00+0000'},
                    ],
                },
                {
                    'name': 'follower_count',
                    'values': [
                        {'value': '700', 'end_time': '2026-02-01T07:00:00+0000'},
                        {'value': '710', 'end_time': '2026-02-02T07:00:00+0000'},
                    ],
                },
            ]
        }

        points = orchestrator._parse_instagram_account_daily_insights(payload)

        self.assertEqual(
            points,
            [
                {
                    'created_at': date(2026, 2, 1),
                    'accounts_reached': 0,
                    'impressions': 120,
                    'profile_views': 0,
                    'accounts_engaged': 20,
                    'follower_count': 700,
                    'follows_and_unfollows': 0,
                },
                {
                    'created_at': date(2026, 2, 2),
                    'accounts_reached': 0,
                    'impressions': 140,
                    'profile_views': 0,
                    'accounts_engaged': 26,
                    'follower_count': 710,
                    'follows_and_unfollows': 0,
                },
            ],
        )


class InstagramDashboardApiTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='ig-user', password='Secret123!')
        self.client = Client()
        self.client.force_login(self.user)
        self.dashboard_user = DashboardUser.objects.create(
            user=self.user,
            id_meta_user='meta-ig-user',
            long_access_token='token',
        )
        self.page = FacebookPage.objects.create(
            id_meta_page='page_1',
            name='Pagina 1',
            dashboard_user_id=self.dashboard_user,
        )
        self.account = InstagramAccount.objects.create(
            id_meta_instagram='ig_1',
            id_page=self.page,
            name='perfil_1',
            accounts_reached=999,
            impressions=888,
            accounts_engaged=777,
            follower_count=555,
        )
        InstagramAccountInsightDaily.objects.create(
            id_meta_instagram=self.account,
            created_at=date(2026, 2, 1),
            impressions=100,
            accounts_engaged=25,
            follower_count=500,
        )
        InstagramAccountInsightDaily.objects.create(
            id_meta_instagram=self.account,
            created_at=date(2026, 2, 2),
            impressions=120,
            accounts_engaged=30,
            follower_count=510,
        )
        MediaInstagram.objects.create(
            id_meta_media='media_1',
            id_meta_instagram=self.account,
            media_type='IMAGE',
            permalink='https://example.com/media_1',
            timestamp=datetime(2026, 2, 2, tzinfo=timezone.utc),
            likes=40,
            comments=7,
            saved=9,
            shares=3,
            reach=80,
            views=90,
        )

    def test_instagram_timeseries_returns_daily_points(self):
        response = self.client.get(
            '/api/instagram/timeseries',
            {'date_start': '2026-02-01', 'date_end': '2026-02-03', 'instagram_account_id': 'ig_1'},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()['timeseries'],
            [
                {'date': '2026-02-01', 'impressions': 100, 'interactions': 25, 'followers': 500},
                {'date': '2026-02-02', 'impressions': 120, 'interactions': 30, 'followers': 510},
                {'date': '2026-02-03', 'impressions': 0, 'interactions': 0, 'followers': None},
            ],
        )

    def test_instagram_kpis_uses_daily_insights_and_latest_followers(self):
        response = self.client.get(
            '/api/instagram/kpis',
            {'date_start': '2026-02-01', 'date_end': '2026-02-03', 'instagram_account_id': 'ig_1'},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()['kpis']
        self.assertEqual(payload['impressoes'], 220)
        self.assertEqual(payload['interacoes'], 55)
        self.assertEqual(payload['seguidores'], 510)
        self.assertEqual(payload['curtidas'], 40)
        self.assertEqual(payload['comentarios'], 7)
