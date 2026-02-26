import re
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterable, List, Optional

from Dashboard.models import DashboardUser
from Dashboard.services.meta_client import MetaClientError, MetaGraphClient

from .models import Cliente


DISPLAY_AMOUNT_RE = re.compile(r'R\$\s*([0-9][0-9\.,]*)', re.IGNORECASE)


def _skip_result(detail: str) -> Dict:
    return {
        'updated_clientes': 0,
        'total_clientes': 0,
        'total_ad_accounts': 0,
        'error_count': 0,
        'parse_error_count': 0,
        'skipped': True,
        'detail': detail,
    }


def _parse_decimal_from_display_string(display_string: str) -> Optional[Decimal]:
    candidate = str(display_string or '').strip()
    if not candidate:
        return None

    match = DISPLAY_AMOUNT_RE.search(candidate)
    if match is None:
        return None

    number_text = str(match.group(1) or '').strip().replace(' ', '')
    if not number_text:
        return None

    if ',' in number_text and '.' in number_text:
        if number_text.rfind(',') > number_text.rfind('.'):
            normalized = number_text.replace('.', '').replace(',', '.')
        else:
            normalized = number_text.replace(',', '')
    elif ',' in number_text:
        normalized = number_text.replace('.', '').replace(',', '.')
    else:
        normalized = number_text.replace(',', '')

    try:
        return Decimal(normalized).quantize(Decimal('0.01'))
    except (InvalidOperation, ValueError):
        return None


def _build_funding_calls(ad_account_ids: Iterable[str]) -> List[Dict[str, str]]:
    calls: List[Dict[str, str]] = []
    for ad_account_id in ad_account_ids:
        clean_id = str(ad_account_id or '').strip()
        if not clean_id:
            continue
        calls.append(
            {
                'method': 'GET',
                'relative_url': f'{clean_id}?fields=funding_source_details',
            }
        )
    return calls


def _resolve_meta_dashboard_user_and_token(user):
    dashboard_user = DashboardUser.objects.filter(user=user).first()
    if dashboard_user is None:
        return None, None, _skip_result('DashboardUser nao encontrado para o usuario autenticado.')

    access_token = str(dashboard_user.long_access_token or '').strip()
    if not access_token:
        return None, None, _skip_result('Long token ausente.')

    if not dashboard_user.has_valid_long_token():
        return None, None, _skip_result('Long token ausente ou expirado.')

    return dashboard_user, access_token, None


def sync_clientes_saldo_atual_from_meta(user, *, batch_size: int = 50) -> Dict:
    dashboard_user, access_token, skip_response = _resolve_meta_dashboard_user_and_token(user)
    if skip_response is not None:
        return skip_response

    clientes = list(
        Cliente.objects.select_related('nome')
        .filter(nome__id_dashboard_user=dashboard_user)
        .order_by('-created_at')
    )
    if not clientes:
        return {
            'updated_clientes': 0,
            'total_clientes': 0,
            'total_ad_accounts': 0,
            'error_count': 0,
            'parse_error_count': 0,
            'skipped': False,
            'detail': 'Nenhum cliente para sincronizar.',
        }

    ad_account_to_clientes: Dict[str, List[Cliente]] = {}
    for cliente in clientes:
        ad_account_id = str(cliente.nome.id_meta_ad_account or '').strip()
        if not ad_account_id:
            continue
        ad_account_to_clientes.setdefault(ad_account_id, []).append(cliente)

    ad_account_ids = list(ad_account_to_clientes.keys())
    calls = _build_funding_calls(ad_account_ids)
    if not calls:
        return {
            'updated_clientes': 0,
            'total_clientes': len(clientes),
            'total_ad_accounts': 0,
            'error_count': 0,
            'parse_error_count': 0,
            'skipped': False,
            'detail': 'Nenhuma conta Meta valida para consulta.',
        }

    graph_client = MetaGraphClient(
        access_token=access_token,
        request_pause_seconds=0,
        batch_size=max(1, int(batch_size or 1)),
    )

    try:
        results = graph_client.batch_request(
            calls,
            entity='empresa_clientes_funding_source_details',
            batch_size=max(1, int(batch_size or 1)),
        )
    except MetaClientError as exc:
        return {
            'updated_clientes': 0,
            'total_clientes': len(clientes),
            'total_ad_accounts': len(ad_account_ids),
            'error_count': len(ad_account_ids),
            'parse_error_count': 0,
            'skipped': False,
            'detail': str(exc),
        }

    parsed_amount_by_ad_account: Dict[str, Decimal] = {}
    error_count = 0
    parse_error_count = 0

    for ad_account_id, result in zip(ad_account_ids, results):
        status_code = int(result.get('status_code') or 0)
        if status_code >= 400:
            error_count += 1
            continue

        body = result.get('body')
        if not isinstance(body, dict):
            parse_error_count += 1
            continue

        funding_source_details = body.get('funding_source_details')
        if not isinstance(funding_source_details, dict):
            parse_error_count += 1
            continue

        display_string = str(funding_source_details.get('display_string') or '').strip()
        parsed_amount = _parse_decimal_from_display_string(display_string)
        if parsed_amount is None:
            parse_error_count += 1
            continue

        parsed_amount_by_ad_account[ad_account_id] = parsed_amount

    clientes_to_update: List[Cliente] = []
    for ad_account_id, parsed_amount in parsed_amount_by_ad_account.items():
        linked_clientes = ad_account_to_clientes.get(ad_account_id) or []
        for cliente in linked_clientes:
            if cliente.saldo_atual == parsed_amount:
                continue
            cliente.saldo_atual = parsed_amount
            clientes_to_update.append(cliente)

    if clientes_to_update:
        Cliente.objects.bulk_update(clientes_to_update, ['saldo_atual'])

    return {
        'updated_clientes': len(clientes_to_update),
        'total_clientes': len(clientes),
        'total_ad_accounts': len(ad_account_ids),
        'error_count': error_count,
        'parse_error_count': parse_error_count,
        'skipped': False,
        'detail': 'Sincronizacao de saldo concluida.',
    }
