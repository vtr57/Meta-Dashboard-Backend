from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from Dashboard.models import AdAccount, DashboardUser
from empresa.meta_funding_service import (
    _parse_decimal_from_display_string,
    sync_clientes_saldo_atual_from_meta,
)
from empresa.models import Cliente


User = get_user_model()


class MetaFundingServiceTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='empresa-user', password='Secret123!')
        self.dashboard_user = DashboardUser.objects.create(
            user=self.user,
            id_meta_user='meta-user-empresa',
            long_access_token='long-token-empresa',
        )
        self.ad_account = AdAccount.objects.create(
            id_meta_ad_account='act_1261766274642927',
            name='Conta Meta Empresa',
            id_dashboard_user=self.dashboard_user,
        )

    def _create_cliente(self, *, name: str, saldo_atual: str = '0.00') -> Cliente:
        return Cliente.objects.create(
            name=name,
            nicho_atuacao='Servico',
            valor_investido=Decimal('100.00'),
            forma_pagamento=Cliente.FORMA_PAGAMENTO_PIX,
            periodo_cobranca=Cliente.PERIODO_COBRANCA_MENSAL,
            saldo_atual=Decimal(saldo_atual),
            gasto_diario=Decimal('10.00'),
            nome=self.ad_account,
            data_renovacao_creditos=date(2026, 2, 1),
        )

    def test_parse_display_string_brl_into_decimal(self):
        parsed = _parse_decimal_from_display_string('Saldo disponivel (R$193,47 BRL)')
        self.assertEqual(parsed, Decimal('193.47'))

    @patch('empresa.meta_funding_service.MetaGraphClient.batch_request', autospec=True)
    def test_sync_uses_single_batch_call_for_multiple_clientes_same_ad_account(self, mocked_batch_request):
        cliente_a = self._create_cliente(name='Cliente A', saldo_atual='0.00')
        cliente_b = self._create_cliente(name='Cliente B', saldo_atual='1.00')
        mocked_batch_request.return_value = [
            {
                'status_code': 200,
                'body': {
                    'funding_source_details': {
                        'id': '5464668763648347',
                        'display_string': 'Saldo disponivel (R$193,47 BRL)',
                        'type': 20,
                    },
                    'id': self.ad_account.id_meta_ad_account,
                },
                'body_raw': '',
                'headers': [],
            }
        ]

        result = sync_clientes_saldo_atual_from_meta(self.user)

        self.assertEqual(mocked_batch_request.call_count, 1)
        batch_calls = mocked_batch_request.call_args[0][1]
        self.assertEqual(len(batch_calls), 1)
        self.assertEqual(
            batch_calls[0]['relative_url'],
            f'{self.ad_account.id_meta_ad_account}?fields=funding_source_details',
        )

        cliente_a.refresh_from_db()
        cliente_b.refresh_from_db()
        self.assertEqual(cliente_a.saldo_atual, Decimal('193.47'))
        self.assertEqual(cliente_b.saldo_atual, Decimal('193.47'))
        self.assertEqual(result['updated_clientes'], 2)
        self.assertEqual(result['error_count'], 0)
        self.assertEqual(result['parse_error_count'], 0)

    @patch('empresa.meta_funding_service.MetaGraphClient.batch_request', autospec=True)
    def test_sync_ignores_rows_without_funding_source_details(self, mocked_batch_request):
        cliente = self._create_cliente(name='Cliente Sem Funding', saldo_atual='55.00')
        mocked_batch_request.return_value = [
            {
                'status_code': 200,
                'body': {'id': self.ad_account.id_meta_ad_account},
                'body_raw': '',
                'headers': [],
            }
        ]

        result = sync_clientes_saldo_atual_from_meta(self.user)

        cliente.refresh_from_db()
        self.assertEqual(cliente.saldo_atual, Decimal('55.00'))
        self.assertEqual(result['updated_clientes'], 0)
        self.assertEqual(result['error_count'], 0)
        self.assertEqual(result['parse_error_count'], 1)


class ClientesEndpointRefreshSaldoTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='empresa-api-user', password='Secret123!')
        self.dashboard_user = DashboardUser.objects.create(
            user=self.user,
            id_meta_user='meta-user-api',
            long_access_token='expired-long-token',
            expired_at=timezone.now() - timedelta(days=1),
        )
        self.ad_account = AdAccount.objects.create(
            id_meta_ad_account='act_900000000000000',
            name='Conta API',
            id_dashboard_user=self.dashboard_user,
        )
        self.cliente = Cliente.objects.create(
            name='Cliente API',
            nicho_atuacao='Agencia',
            valor_investido=Decimal('300.00'),
            forma_pagamento=Cliente.FORMA_PAGAMENTO_CARTAO_CREDITO,
            periodo_cobranca=Cliente.PERIODO_COBRANCA_SEMANAL,
            saldo_atual=Decimal('30.00'),
            gasto_diario=Decimal('15.00'),
            nome=self.ad_account,
            data_renovacao_creditos=date(2026, 2, 10),
        )
        self.client.force_login(self.user)

    def test_get_clientes_refresh_saldo_with_invalid_token_does_not_break_listing(self):
        response = self.client.get('/api/empresa/clientes', {'refresh_saldo': '1'})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['total'], 1)
        self.assertEqual(len(payload['clientes']), 1)
        self.assertEqual(payload['clientes'][0]['id'], self.cliente.id)
        self.assertIn('saldo_sync', payload)
        self.assertTrue(payload['saldo_sync']['skipped'])
