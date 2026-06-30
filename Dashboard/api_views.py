from datetime import timedelta
import logging
import re
import threading
from decimal import Decimal, InvalidOperation
from typing import Optional

import pandas as pd
from scipy.stats import pearsonr
from django.db.models import Sum
from django.utils import timezone
from django.utils.dateparse import parse_date
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

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
    InstagramAccount,
    InstagramAccountInsightDaily,
    MediaInstagram,
    SyncLog,
    SyncRun,
)
from Dashboard.serializers import AnotacoesSerializer, MetaSpecificInsightsSerializer
from Dashboard.services.meta_client import MetaClientError, MetaGraphClient
from Dashboard.services.meta_sync_orchestrator import MetaSyncOrchestrator
from Dashboard.services.statistics_clustering_service import build_clustering_analysis
from Dashboard.services.statistics_service import build_statistics_analysis
from Dashboard.services.statistics_time_series_service import (
    METRIC_CONFIG as TIME_SERIES_METRIC_CONFIG,
    build_time_series_analysis,
)


logger = logging.getLogger(__name__)
OWNER_RE = re.compile(r'user_id=(\d+)')


def _run_sync_in_background(
    sync_run_id: int,
    dashboard_user_id: int,
    sync_scope: str = 'all',
    insights_days_override: Optional[int] = None,
    instagram_account_id: Optional[str] = None,
    date_start=None,
    date_end=None,
) -> None:
    try:
        MetaSyncOrchestrator(
            sync_run_id=sync_run_id,
            dashboard_user_id=dashboard_user_id,
            sync_scope=sync_scope,
            insights_days_override=insights_days_override,
            instagram_account_id=instagram_account_id,
            date_start=date_start,
            date_end=date_end,
        ).run()
    except Exception:
        logger.exception('Unexpected failure in sync background thread.')


def _sync_belongs_to_user(sync_run: SyncRun, user_id: int) -> bool:
    owner_message = (
        SyncLog.objects.filter(sync_run=sync_run, entidade='sync_owner')
        .order_by('id')
        .values_list('mensagem', flat=True)
        .first()
    )
    if not owner_message:
        return True
    match = OWNER_RE.search(owner_message)
    if not match:
        return True
    return int(match.group(1)) == int(user_id)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def meta_connection_status(request):
    dashboard_user = DashboardUser.objects.filter(user=request.user).first()
    if dashboard_user is None:
        return Response(
            {
                'connected': False,
                'id_meta_user': None,
                'expired_at': None,
                'has_valid_long_token': False,
                'sync_requires_reconnect': True,
            },
            status=status.HTTP_200_OK,
        )

    has_valid = dashboard_user.has_valid_long_token()
    return Response(
        {
            'connected': True,
            'id_meta_user': dashboard_user.id_meta_user,
            'expired_at': dashboard_user.expired_at,
            'has_valid_long_token': has_valid,
            'sync_requires_reconnect': not has_valid,
        },
        status=status.HTTP_200_OK,
    )


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def meta_sync_start(request):
    date_start, date_end, date_error = _parse_optional_sync_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)
    return _start_sync(request, sync_scope='all', date_start=date_start, date_end=date_end)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def meta_sync_start_meta(request):
    date_start, date_end, date_error = _parse_optional_sync_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)
    return _start_sync(request, sync_scope='meta', date_start=date_start, date_end=date_end)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def meta_sync_start_instagram(request):
    date_start, date_end, date_error = _parse_optional_sync_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)
    return _start_sync(request, sync_scope='instagram', date_start=date_start, date_end=date_end)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def instagram_sync_selected(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    if not dashboard_user.has_valid_long_token():
        return Response(
            {
                'detail': 'Long token ausente ou expirado. Reconecte antes de sincronizar.',
                'sync_requires_reconnect': True,
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    instagram_account_id = str(request.data.get('instagram_account_id') or '').strip()
    if not instagram_account_id:
        return Response({'detail': 'Selecione uma conta de Instagram.'}, status=status.HTTP_400_BAD_REQUEST)

    account = _instagram_accounts_queryset(dashboard_user).filter(id_meta_instagram=instagram_account_id).first()
    if account is None:
        return Response({'detail': 'Conta de Instagram invalida para este usuario.'}, status=status.HTTP_400_BAD_REQUEST)

    date_start, date_end, date_error = _parse_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)

    return _start_sync(
        request,
        sync_scope='instagram',
        instagram_account_id=account.id_meta_instagram,
        date_start=date_start,
        date_end=date_end,
    )


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def meta_sync_start_insights_7d(request):
    return _start_sync(request, sync_scope='all', insights_days_override=7)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def meta_sync_start_insights_1d(request):
    return _start_sync(request, sync_scope='meta', insights_days_override=1)


def _start_sync(
    request,
    sync_scope: str,
    insights_days_override: Optional[int] = None,
    instagram_account_id: Optional[str] = None,
    date_start=None,
    date_end=None,
):
    dashboard_user = DashboardUser.objects.filter(user=request.user).first()
    if dashboard_user is None:
        return Response(
            {
                'detail': 'Usuario nao conectado na Meta. Conecte antes de sincronizar.',
                'sync_requires_reconnect': True,
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    if not dashboard_user.has_valid_long_token():
        return Response(
            {
                'detail': 'Long token ausente ou expirado. Reconecte antes de sincronizar.',
                'sync_requires_reconnect': True,
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    sync_run = SyncRun.objects.create(status=SyncRun.Status.PENDING)
    SyncLog.objects.create(
        sync_run=sync_run,
        entidade='sync_owner',
        mensagem=(
            f'user_id={request.user.id};dashboard_user_id={dashboard_user.id};'
            f'instagram_account_id={instagram_account_id or ""};'
            f'date_start={date_start.isoformat() if date_start else ""};'
            f'date_end={date_end.isoformat() if date_end else ""}'
        ),
    )
    SyncLog.objects.create(
        sync_run=sync_run,
        entidade='sync',
        mensagem=(
            f'Sincronizacao enfileirada. Escopo={sync_scope}.'
            + (f' Conta Instagram={instagram_account_id}.' if instagram_account_id else '')
            + (
                f' Periodo={date_start.isoformat()}..{date_end.isoformat()}.'
                if date_start is not None and date_end is not None
                else ''
            )
            + (
                f' Janela de insights: ultimos {insights_days_override} dias.'
                if insights_days_override is not None
                else ''
            )
        ),
    )

    thread = threading.Thread(
        target=_run_sync_in_background,
        args=(sync_run.id, dashboard_user.id, sync_scope, insights_days_override, instagram_account_id, date_start, date_end),
        daemon=True,
        name=f'meta-sync-{sync_scope}-{sync_run.id}',
    )
    thread.start()

    return Response(
        {
            'detail': 'Sincronizacao iniciada.',
            'sync_run_id': sync_run.id,
            'status': sync_run.status,
            'sync_scope': sync_scope,
            'insights_days_override': insights_days_override,
            'instagram_account_id': instagram_account_id,
            'date_start': date_start,
            'date_end': date_end,
            'sync_requires_reconnect': False,
        },
        status=status.HTTP_202_ACCEPTED,
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def meta_sync_logs(request, sync_run_id: int):
    sync_run = SyncRun.objects.filter(id=sync_run_id).first()
    if sync_run is None:
        return Response({'detail': 'SyncRun nao encontrado.'}, status=status.HTTP_404_NOT_FOUND)

    if not _sync_belongs_to_user(sync_run, request.user.id):
        return Response({'detail': 'Sem permissao para este SyncRun.'}, status=status.HTTP_403_FORBIDDEN)

    try:
        since_id = int(request.query_params.get('since_id', 0))
    except (TypeError, ValueError):
        since_id = 0
    try:
        limit = int(request.query_params.get('limit', 200))
    except (TypeError, ValueError):
        limit = 200
    limit = max(1, min(limit, 1000))

    logs_qs = (
        SyncLog.objects.filter(sync_run=sync_run, id__gt=since_id)
        .exclude(entidade='sync_owner')
        .order_by('id')[:limit]
    )
    logs = [
        {
            'id': log.id,
            'entidade': log.entidade,
            'mensagem': log.mensagem,
            'timestamp': log.timestamp,
        }
        for log in logs_qs
    ]
    next_since_id = logs[-1]['id'] if logs else since_id
    is_finished = sync_run.status in {SyncRun.Status.SUCCESS, SyncRun.Status.FAILED}

    return Response(
        {
            'sync_run': {
                'id': sync_run.id,
                'status': sync_run.status,
                'started_at': sync_run.started_at,
                'finished_at': sync_run.finished_at,
                'is_finished': is_finished,
            },
            'logs': logs,
            'next_since_id': next_since_id,
            'poll_interval_seconds': 2,
        },
        status=status.HTTP_200_OK,
    )


def _to_int(value) -> int:
    if value in (None, ''):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _to_float(value) -> float:
    if value in (None, ''):
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_div(numerator, denominator, multiplier=1.0) -> float:
    den = _to_float(denominator)
    if den <= 0:
        return 0.0
    return (_to_float(numerator) / den) * multiplier


def _meta_spend_results_correlation(qs) -> Optional[float]:
    rows = list(
        qs.values('created_at')
        .annotate(
            spend_total=Sum('gasto_diario'),
            results_total=Sum('quantidade_results_diaria'),
        )
        .order_by('created_at')
    )
    if len(rows) < 2:
        return None

    frame = pd.DataFrame(rows)
    if frame.empty:
        return None

    frame['spend_total'] = pd.to_numeric(frame['spend_total'], errors='coerce')
    frame['results_total'] = pd.to_numeric(frame['results_total'], errors='coerce')
    frame = frame.dropna(subset=['spend_total', 'results_total'])

    if len(frame) < 2:
        return None
    if frame['spend_total'].nunique() < 2:
        return None
    if frame['results_total'].nunique() < 2:
        return None

    correlation, _ = pearsonr(frame['spend_total'], frame['results_total'])
    if pd.isna(correlation):
        return None
    return float(correlation)


def _parse_date_range(request):
    today = timezone.localdate()
    request_data = getattr(request, 'data', {}) if hasattr(request, 'data') else {}
    start_raw = (
        request.query_params.get('date_start')
        or request.query_params.get('start_date')
        or request_data.get('date_start')
        or request_data.get('start_date')
    )
    end_raw = (
        request.query_params.get('date_end')
        or request.query_params.get('end_date')
        or request_data.get('date_end')
        or request_data.get('end_date')
    )
    start = parse_date(start_raw) if start_raw else None
    end = parse_date(end_raw) if end_raw else None

    if start_raw and start is None:
        return None, None, 'date_start invalida. Use formato YYYY-MM-DD.'
    if end_raw and end is None:
        return None, None, 'date_end invalida. Use formato YYYY-MM-DD.'

    if end is None:
        end = today
    if start is None:
        start = end - timedelta(days=30)
    if start > end:
        return None, None, 'date_start nao pode ser maior que date_end.'
    return start, end, None


def _parse_optional_sync_date_range(request):
    request_data = getattr(request, 'data', {}) if hasattr(request, 'data') else {}
    start_raw = (
        request.query_params.get('date_start')
        or request.query_params.get('start_date')
        or request_data.get('date_start')
        or request_data.get('start_date')
    )
    end_raw = (
        request.query_params.get('date_end')
        or request.query_params.get('end_date')
        or request_data.get('date_end')
        or request_data.get('end_date')
    )

    if not start_raw and not end_raw:
        return None, None, None
    if not start_raw or not end_raw:
        return None, None, 'Informe data inicial e data final para usar um periodo personalizado.'

    start = parse_date(start_raw)
    end = parse_date(end_raw)
    if start is None:
        return None, None, 'date_start invalida. Use formato YYYY-MM-DD.'
    if end is None:
        return None, None, 'date_end invalida. Use formato YYYY-MM-DD.'
    if start > end:
        return None, None, 'date_start nao pode ser maior que date_end.'

    return start, end, None


def _get_dashboard_user_or_error(request):
    dashboard_user = DashboardUser.objects.filter(user=request.user).first()
    if dashboard_user is None:
        return None, Response(
            {'detail': 'Usuario nao conectado na Meta.'},
            status=status.HTTP_400_BAD_REQUEST,
        )
    return dashboard_user, None


def _get_meta_filter_values(request):
    def _parse_query_values(param_name):
        raw_values = []
        raw_values.extend(request.query_params.getlist(param_name))
        raw_values.extend(request.query_params.getlist(f'{param_name}[]'))
        raw_values.extend(request.query_params.getlist(f'{param_name}s'))
        raw_values.extend(request.query_params.getlist(f'{param_name}s[]'))
        if not raw_values:
            single_value = (
                request.query_params.get(param_name)
                or request.query_params.get(f'{param_name}[]')
                or request.query_params.get(f'{param_name}s')
                or request.query_params.get(f'{param_name}s[]')
                or ''
            )
            if isinstance(single_value, str) and ',' in single_value:
                raw_values.extend(single_value.split(','))
            elif single_value:
                raw_values.append(single_value)

        normalized = []
        seen = set()
        for raw in raw_values:
            value = str(raw or '').strip()
            if not value or value in seen:
                continue
            seen.add(value)
            normalized.append(value)
        return normalized

    return {
        'ad_account_ids': _parse_query_values('ad_account_id'),
        'campaign_ids': _parse_query_values('campaign_id'),
        'adset_ids': _parse_query_values('adset_id'),
        'ad_ids': _parse_query_values('ad_id'),
    }


def _budget_minor_to_major(value):
    raw = str(value or '').strip()
    if not raw:
        return None
    try:
        parsed = Decimal(raw)
    except (InvalidOperation, TypeError, ValueError):
        return None
    return float(parsed / Decimal('100'))


def _round_or_none(value, digits=4):
    if value is None:
        return None
    return round(float(value), digits)


def _sum_action_values(rows, accepted_keys):
    total = Decimal('0')
    found = False
    normalized_keys = {str(key or '').strip().lower() for key in accepted_keys}

    for row in rows or []:
        if not isinstance(row, dict):
            continue

        action_type = str(row.get('action_type') or row.get('indicator') or '').strip().lower()
        if action_type in normalized_keys:
            values = row.get('values')
            if isinstance(values, list):
                for value_row in values:
                    if not isinstance(value_row, dict):
                        continue
                    try:
                        total += Decimal(str(value_row.get('value') or '0'))
                        found = True
                    except (InvalidOperation, TypeError, ValueError):
                        continue
                continue

            try:
                total += Decimal(str(row.get('value') or '0'))
                found = True
            except (InvalidOperation, TypeError, ValueError):
                continue

    if not found:
        return None
    return float(total)


def _extract_live_insights_metrics(payload):
    metrics = {
        'video_3s_views': None,
        'messaging_conversations_started': None,
    }
    data_rows = payload.get('data') if isinstance(payload, dict) else None
    if not isinstance(data_rows, list):
        return metrics

    video_total = Decimal('0')
    video_found = False
    messaging_total = Decimal('0')
    messaging_found = False

    for row in data_rows:
        if not isinstance(row, dict):
            continue

        video_value = _sum_action_values(
            row.get('video_3_sec_watched_actions') or [],
            {'video_view', 'video_3_sec_watched_actions'},
        )
        if video_value is not None:
            video_total += Decimal(str(video_value))
            video_found = True

        messaging_value = _sum_action_values(
            row.get('actions') or [],
            {
                'onsite_conversion.messaging_conversation_started_7d',
                'onsite_conversion.messaging_first_reply',
                'onsite_conversion.total_messaging_connection',
                'onsite_conversion.total_messaging_connection_7d',
            },
        )
        if messaging_value is not None:
            messaging_total += Decimal(str(messaging_value))
            messaging_found = True

    metrics['video_3s_views'] = float(video_total) if video_found else None
    metrics['messaging_conversations_started'] = float(messaging_total) if messaging_found else None
    return metrics


def _make_meta_client_for_dashboard_user(dashboard_user):
    return MetaGraphClient(
        access_token=dashboard_user.long_access_token,
        request_pause_seconds=0.0,
        max_retries=2,
        batch_size=50,
    )


def _fetch_campaign_budget_from_graph(client, campaign_id):
    payload = client.request_with_retry(
        'GET',
        campaign_id,
        params={'fields': 'daily_budget,lifetime_budget,budget_remaining'},
        entity='meta_report_budget',
    )
    for field in ('daily_budget', 'lifetime_budget', 'budget_remaining'):
        budget = _budget_minor_to_major(payload.get(field) if isinstance(payload, dict) else None)
        if budget is not None:
            return budget
    return None


def _fetch_account_budgets_from_graph(client, ad_account_id):
    total = Decimal('0')
    found = False
    for row in client.paginate(
        f'{ad_account_id}/campaigns',
        params={'fields': 'daily_budget,lifetime_budget,budget_remaining', 'limit': 200},
        entity='meta_report_budget',
    ):
        for field in ('daily_budget', 'lifetime_budget', 'budget_remaining'):
            budget = _budget_minor_to_major(row.get(field) if isinstance(row, dict) else None)
            if budget is not None:
                total += Decimal(str(budget))
                found = True
                break
    if not found:
        return None
    return float(total)


def _fetch_live_report_metrics(
    dashboard_user,
    accessible_accounts,
    *,
    ad_account_ids,
    campaign_ids,
    date_start,
    date_end,
    include_budget=True,
):
    if not dashboard_user.has_valid_long_token():
        raise MetaClientError('Long token ausente ou expirado.')

    client = _make_meta_client_for_dashboard_user(dashboard_user)
    time_range = {'since': date_start.isoformat(), 'until': date_end.isoformat()}
    scope_ids = []

    if campaign_ids:
        scope_ids = list(campaign_ids)
    elif ad_account_ids:
        scope_ids = list(ad_account_ids)
    else:
        scope_ids = list(accessible_accounts.values_list('id_meta_ad_account', flat=True))

    metrics = {
        'budget': None,
        'video_3s_views': None,
        'messaging_conversations_started': None,
    }

    if include_budget:
        try:
            if campaign_ids:
                budget_total = Decimal('0')
                budget_found = False
                for campaign_id in campaign_ids:
                    campaign_budget = _fetch_campaign_budget_from_graph(client, campaign_id)
                    if campaign_budget is None:
                        continue
                    budget_total += Decimal(str(campaign_budget))
                    budget_found = True
                metrics['budget'] = float(budget_total) if budget_found else None
            else:
                budget_total = Decimal('0')
                budget_found = False
                for account_id in ad_account_ids or scope_ids:
                    account_budget = _fetch_account_budgets_from_graph(client, account_id)
                    if account_budget is None:
                        continue
                    budget_total += Decimal(str(account_budget))
                    budget_found = True
                metrics['budget'] = float(budget_total) if budget_found else None
        except MetaClientError:
            logger.exception('Falha ao consultar orcamento do relatorio Meta.')

    try:
        video_total = Decimal('0')
        video_found = False
        messaging_total = Decimal('0')
        messaging_found = False

        for scope_id in scope_ids:
            payload = client.request_with_retry(
                'GET',
                f'{scope_id}/insights',
                params={
                    'fields': 'actions,video_3_sec_watched_actions',
                    'time_range': f'{{"since":"{time_range["since"]}","until":"{time_range["until"]}"}}',
                    'limit': 100,
                },
                entity='meta_report_live_insights',
            )
            current = _extract_live_insights_metrics(payload)
            if current['video_3s_views'] is not None:
                video_total += Decimal(str(current['video_3s_views']))
                video_found = True
            if current['messaging_conversations_started'] is not None:
                messaging_total += Decimal(str(current['messaging_conversations_started']))
                messaging_found = True

        metrics['video_3s_views'] = float(video_total) if video_found else None
        metrics['messaging_conversations_started'] = float(messaging_total) if messaging_found else None
    except MetaClientError:
        logger.exception('Falha ao consultar metricas complementares do relatorio Meta.')

    return metrics


def _build_report_metrics(qs, live_metrics):
    totals = qs.aggregate(
        spend_total=Sum('gasto_diario'),
        impressions_total=Sum('impressao_diaria'),
        reach_total=Sum('alcance_diario'),
        results_total=Sum('quantidade_results_diaria'),
        clicks_total=Sum('quantidade_clicks_diaria'),
    )

    spend_total = _to_float(totals['spend_total'])
    impressions_total = _to_int(totals['impressions_total'])
    reach_total = _to_int(totals['reach_total'])
    results_total = _to_int(totals['results_total'])
    clicks_total = _to_int(totals['clicks_total'])

    messaging_conversations_started = live_metrics['messaging_conversations_started']
    if messaging_conversations_started is None:
        messaging_conversations_started = float(results_total)

    video_3s_views = live_metrics['video_3s_views']
    budget = live_metrics['budget']

    cpr = (spend_total / results_total) if results_total > 0 else None
    cpc = (spend_total / clicks_total) if clicks_total > 0 else None
    ctr = _safe_div(clicks_total, impressions_total, 100.0)
    cpm = _safe_div(spend_total, impressions_total, 1000.0)
    frequency = _safe_div(impressions_total, reach_total, 1.0)
    video_rate = None if video_3s_views is None else _safe_div(video_3s_views, impressions_total, 100.0)
    messaging_conversion_rate = (
        None if messaging_conversations_started is None else _safe_div(messaging_conversations_started, clicks_total, 100.0)
    )

    return {
        'orcamento': _round_or_none(budget, 2),
        'valor_usado': round(spend_total, 4),
        'resultados': results_total,
        'custo_por_resultado': _round_or_none(cpr),
        'cpc_link': _round_or_none(cpc),
        'ctr_link': round(ctr, 4),
        'taxa_video_3s_por_impressoes': _round_or_none(video_rate),
        'tx_conversao_envio_mensagem': _round_or_none(messaging_conversion_rate),
        'cpm': round(cpm, 4),
        'alcance': reach_total,
        'frequencia': round(frequency, 4),
        'impressoes': impressions_total,
        'cliques_link': clicks_total,
        'visualizacoes_video_3s': _round_or_none(video_3s_views),
        'conversas_mensagens_iniciadas': _round_or_none(messaging_conversations_started),
    }


def _get_previous_period_range(date_start, date_end):
    period_days = (date_end - date_start).days + 1
    previous_end = date_start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=period_days - 1)
    return previous_start, previous_end


def _calculate_percent_change(current_value, previous_value):
    if current_value is None or previous_value is None:
        return None

    current = _to_float(current_value)
    previous = _to_float(previous_value)
    if previous == 0.0:
        return 0.0 if current == 0.0 else None

    return ((current - previous) / previous) * 100.0


def _build_metric_changes(current_metrics, previous_metrics):
    changes = {}
    for key, current_value in current_metrics.items():
        if key == 'orcamento':
            continue
        changes[key] = _round_or_none(_calculate_percent_change(current_value, previous_metrics.get(key)))
    return changes


def _ad_accounts_for_dashboard_user(dashboard_user: DashboardUser):
    return AdAccount.objects.accessible_to(dashboard_user)


def _ids_match_queryset(queryset, field_name, ids):
    if not ids:
        return True
    return queryset.filter(**{f'{field_name}__in': ids}).values_list(field_name, flat=True).distinct().count() == len(ids)


def _resolve_meta_filter_context(dashboard_user: DashboardUser, filters: dict):
    selected_ad_account_ids = filters['ad_account_ids']
    selected_campaign_ids = filters['campaign_ids']
    selected_adset_ids = filters['adset_ids']
    selected_ad_ids = filters['ad_ids']

    accessible_accounts = _ad_accounts_for_dashboard_user(dashboard_user)
    if selected_ad_account_ids and not _ids_match_queryset(accessible_accounts, 'id_meta_ad_account', selected_ad_account_ids):
        raise ValueError('Ad account invalido para este usuario.')
    ad_account_scope = (
        accessible_accounts.filter(id_meta_ad_account__in=selected_ad_account_ids)
        if selected_ad_account_ids
        else accessible_accounts
    )

    campaigns_scope = Campaign.objects.filter(id_meta_ad_account__in=ad_account_scope)
    if selected_campaign_ids and not _ids_match_queryset(campaigns_scope, 'id_meta_campaign', selected_campaign_ids):
        raise ValueError('Campaign invalida para este usuario.')
    campaign_scope = (
        campaigns_scope.filter(id_meta_campaign__in=selected_campaign_ids) if selected_campaign_ids else campaigns_scope
    )

    adsets_scope = AdSet.objects.filter(id_meta_campaign__in=campaign_scope)
    if selected_adset_ids and not _ids_match_queryset(adsets_scope, 'id_meta_adset', selected_adset_ids):
        raise ValueError('Adset invalido para este usuario.')
    adset_scope = adsets_scope.filter(id_meta_adset__in=selected_adset_ids) if selected_adset_ids else adsets_scope

    ads_scope = Ad.objects.filter(id_meta_adset__in=adset_scope)
    if selected_ad_ids and not _ids_match_queryset(ads_scope, 'id_meta_ad', selected_ad_ids):
        raise ValueError('Ad invalido para este usuario.')
    ad_scope = ads_scope.filter(id_meta_ad__in=selected_ad_ids) if selected_ad_ids else ads_scope

    return {
        'accessible_accounts': accessible_accounts,
        'ad_account_scope': ad_account_scope,
        'campaigns_scope': campaigns_scope,
        'campaign_scope': campaign_scope,
        'adsets_scope': adsets_scope,
        'adset_scope': adset_scope,
        'ads_scope': ads_scope,
        'ad_scope': ad_scope,
        'filters': filters,
    }


def _serialize_meta_filter_values(filters: dict):
    return {
        'ad_account_ids': filters['ad_account_ids'],
        'campaign_ids': filters['campaign_ids'],
        'adset_ids': filters['adset_ids'],
        'ad_ids': filters['ad_ids'],
    }


def _meta_delivery_status_label(*, effective_status='', status=''):
    normalized = str(effective_status or status or '').strip().lower()
    return 'ATIVO' if normalized == 'active' else 'DESATIVADO'


def _build_meta_insight_queryset(dashboard_user: DashboardUser, filters: dict):
    context = _resolve_meta_filter_context(dashboard_user, filters)
    ad_account_scope = context['ad_account_scope']
    campaign_scope = context['campaign_scope']
    adset_scope = context['adset_scope']
    ad_scope = context['ad_scope']

    if filters['ad_ids']:
        level = 'ad'
        qs = AdInsightDaily.objects.filter(id_meta_ad__in=ad_scope)
        return level, qs

    if filters['adset_ids']:
        level = 'adset'
        qs = AdSetInsightDaily.objects.filter(id_meta_adset__in=adset_scope)
        return level, qs

    if filters['campaign_ids']:
        level = 'campaign'
        qs = CampaignInsightDaily.objects.filter(id_meta_campaign__in=campaign_scope)
        return level, qs

    level = 'ad_account'
    qs = CampaignInsightDaily.objects.filter(id_meta_campaign__id_meta_ad_account__in=ad_account_scope)
    return level, qs


def _build_meta_specific_ad_queryset(dashboard_user: DashboardUser, filters: dict):
    context = _resolve_meta_filter_context(dashboard_user, filters)
    ad_account_scope = context['ad_account_scope']
    campaign_scope = context['campaign_scope']
    adset_scope = context['adset_scope']

    qs = AdInsightDaily.objects.filter(id_meta_ad__id_meta_adset__id_meta_campaign__id_meta_ad_account__in=ad_account_scope)
    level = 'ad_account'

    if filters['campaign_ids']:
        qs = qs.filter(id_meta_ad__id_meta_adset__id_meta_campaign__in=campaign_scope)
        level = 'campaign'
    if filters['adset_ids']:
        qs = qs.filter(id_meta_ad__id_meta_adset__in=adset_scope)
        level = 'adset'

    return level, qs


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def meta_filters(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    filters = _get_meta_filter_values(request)
    try:
        context = _resolve_meta_filter_context(dashboard_user, filters)
    except ValueError as exc:
        return Response({'detail': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

    ad_accounts_qs = context['accessible_accounts'].order_by('name', 'id_meta_ad_account')
    campaigns_qs = context['campaigns_scope'].order_by('name', 'id_meta_campaign')
    adsets_qs = context['adsets_scope'].order_by('name', 'id_meta_adset')
    ads_qs = context['ads_scope'].order_by('name', 'id_meta_ad')

    return Response(
        {
            'ad_accounts': [
                {'id_meta_ad_account': row.id_meta_ad_account, 'name': row.name}
                for row in ad_accounts_qs.only('id_meta_ad_account', 'name')
            ],
            'campaigns': [
                {
                    'id_meta_campaign': row.id_meta_campaign,
                    'id_meta_ad_account': row.id_meta_ad_account.id_meta_ad_account,
                    'name': row.name,
                    'effective_status': row.effective_status,
                    'status': row.status,
                    'status_display': _meta_delivery_status_label(
                        effective_status=row.effective_status,
                        status=row.status,
                    ),
                    'display_name': (
                        f'{row.name} - '
                        f'{_meta_delivery_status_label(effective_status=row.effective_status, status=row.status)}'
                    ).strip(),
                }
                for row in campaigns_qs.select_related('id_meta_ad_account').only(
                    'id_meta_campaign',
                    'name',
                    'status',
                    'effective_status',
                    'id_meta_ad_account__id_meta_ad_account',
                )
            ],
            'adsets': [
                {
                    'id_meta_adset': row.id_meta_adset,
                    'id_meta_campaign': row.id_meta_campaign.id_meta_campaign,
                    'name': row.name,
                    'effective_status': row.effective_status,
                    'status': row.status,
                    'status_display': _meta_delivery_status_label(
                        effective_status=row.effective_status,
                        status=row.status,
                    ),
                    'display_name': (
                        f'{row.name} - '
                        f'{_meta_delivery_status_label(effective_status=row.effective_status, status=row.status)}'
                    ).strip(),
                }
                for row in adsets_qs.select_related('id_meta_campaign').only(
                    'id_meta_adset',
                    'name',
                    'status',
                    'effective_status',
                    'id_meta_campaign__id_meta_campaign',
                )
            ],
            'ads': [
                {
                    'id_meta_ad': row.id_meta_ad,
                    'id_meta_adset': row.id_meta_adset.id_meta_adset,
                    'name': row.name,
                    'effective_status': row.effective_status,
                    'status': row.status,
                    'status_display': _meta_delivery_status_label(
                        effective_status=row.effective_status,
                        status=row.status,
                    ),
                    'display_name': (
                        f'{row.name} - '
                        f'{_meta_delivery_status_label(effective_status=row.effective_status, status=row.status)}'
                    ).strip(),
                }
                for row in ads_qs.select_related('id_meta_adset').only(
                    'id_meta_ad',
                    'name',
                    'status',
                    'effective_status',
                    'id_meta_adset__id_meta_adset',
                )
            ],
            'filters': _serialize_meta_filter_values(filters),
        },
        status=status.HTTP_200_OK,
    )


@api_view(['GET', 'POST'])
@permission_classes([IsAuthenticated])
def meta_anotacoes(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    serializer_context = {'dashboard_user': dashboard_user}

    if request.method == 'GET':
        ad_account_id = str(
            request.query_params.get('ad_account_id') or request.query_params.get('id_meta_ad_account') or ''
        ).strip()
        anotacoes_qs = Anotacoes.objects.filter(
            id_meta_ad_account__in=_ad_accounts_for_dashboard_user(dashboard_user)
        ).select_related('id_meta_ad_account')
        if ad_account_id:
            anotacoes_qs = anotacoes_qs.filter(id_meta_ad_account__id_meta_ad_account=ad_account_id)

        serializer = AnotacoesSerializer(anotacoes_qs, many=True, context=serializer_context)
        return Response({'anotacoes': serializer.data}, status=status.HTTP_200_OK)

    serializer = AnotacoesSerializer(data=request.data, context=serializer_context)
    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    anotacao = serializer.save()
    output = AnotacoesSerializer(anotacao, context=serializer_context)
    return Response({'anotacao': output.data}, status=status.HTTP_201_CREATED)


@api_view(['DELETE'])
@permission_classes([IsAuthenticated])
def meta_anotacao_delete(request, anotacao_id: int):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    anotacao = Anotacoes.objects.filter(
        id=anotacao_id,
        id_meta_ad_account__in=_ad_accounts_for_dashboard_user(dashboard_user),
    ).first()
    if anotacao is None:
        return Response({'detail': 'Anotacao nao encontrada.'}, status=status.HTTP_404_NOT_FOUND)

    anotacao.delete()
    return Response(status=status.HTTP_204_NO_CONTENT)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def meta_timeseries(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    date_start, date_end, date_error = _parse_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)

    filters = _get_meta_filter_values(request)
    try:
        level, qs = _build_meta_insight_queryset(dashboard_user, filters)
    except ValueError as exc:
        return Response({'detail': str(exc)}, status=status.HTTP_400_BAD_REQUEST)
    qs = qs.filter(created_at__gte=date_start, created_at__lte=date_end)

    rows = (
        qs.values('created_at')
        .annotate(
            spend_total=Sum('gasto_diario'),
            impressions_total=Sum('impressao_diaria'),
            reach_total=Sum('alcance_diario'),
            results_total=Sum('quantidade_results_diaria'),
            clicks_total=Sum('quantidade_clicks_diaria'),
        )
        .order_by('created_at')
    )

    series = [
        {
            'date': row['created_at'],
            'spend': _to_float(row['spend_total']),
            'impressions': _to_int(row['impressions_total']),
            'reach': _to_int(row['reach_total']),
            'results': _to_int(row['results_total']),
            'clicks': _to_int(row['clicks_total']),
        }
        for row in rows
    ]

    return Response(
        {
            'level': level,
            'date_start': date_start,
            'date_end': date_end,
            'filters': _serialize_meta_filter_values(filters),
            'series': series,
        },
        status=status.HTTP_200_OK,
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def meta_kpis(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    date_start, date_end, date_error = _parse_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)

    filters = _get_meta_filter_values(request)
    try:
        level, qs = _build_meta_insight_queryset(dashboard_user, filters)
    except ValueError as exc:
        return Response({'detail': str(exc)}, status=status.HTTP_400_BAD_REQUEST)
    qs = qs.filter(created_at__gte=date_start, created_at__lte=date_end)

    totals = qs.aggregate(
        spend_total=Sum('gasto_diario'),
        impressions_total=Sum('impressao_diaria'),
        reach_total=Sum('alcance_diario'),
        results_total=Sum('quantidade_results_diaria'),
        clicks_total=Sum('quantidade_clicks_diaria'),
    )

    spend_total = _to_float(totals['spend_total'])
    impressions_total = _to_int(totals['impressions_total'])
    reach_total = _to_int(totals['reach_total'])
    results_total = _to_int(totals['results_total'])
    clicks_total = _to_int(totals['clicks_total'])

    ctr_medio = _safe_div(clicks_total, impressions_total, 100.0)
    cpm_medio = _safe_div(spend_total, impressions_total, 1000.0)
    cpc_medio = _safe_div(spend_total, clicks_total, 1.0)
    frequencia_media = _safe_div(impressions_total, reach_total, 1.0)
    correlacao_gasto_resultados = _meta_spend_results_correlation(qs)

    return Response(
        {
            'level': level,
            'date_start': date_start,
            'date_end': date_end,
            'filters': _serialize_meta_filter_values(filters),
            'kpis': {
                'gasto_total': round(spend_total, 4),
                'impressao_total': impressions_total,
                'alcance_total': reach_total,
                'results_total': results_total,
                'clicks_total': clicks_total,
                'ctr_medio': round(ctr_medio, 4),
                'cpm_medio': round(cpm_medio, 4),
                'cpc_medio': round(cpc_medio, 4),
                'frequencia_media': round(frequencia_media, 4),
                'correlacao_gasto_resultados': (
                    round(correlacao_gasto_resultados, 4)
                    if correlacao_gasto_resultados is not None
                    else None
                ),
            },
        },
        status=status.HTTP_200_OK,
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def meta_report_summary(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    date_start, date_end, date_error = _parse_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)

    filters = _get_meta_filter_values(request)
    try:
        context = _resolve_meta_filter_context(dashboard_user, filters)
    except ValueError as exc:
        return Response({'detail': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

    accessible_accounts = context['accessible_accounts']
    ad_account_scope = context['ad_account_scope']
    campaign_scope = context['campaign_scope']

    qs = CampaignInsightDaily.objects.filter(
        id_meta_campaign__id_meta_ad_account__in=accessible_accounts,
        created_at__gte=date_start,
        created_at__lte=date_end,
    )
    if filters['campaign_ids']:
        qs = qs.filter(id_meta_campaign__in=campaign_scope)
    else:
        qs = qs.filter(id_meta_campaign__id_meta_ad_account__in=ad_account_scope)

    live_metrics = _fetch_live_report_metrics(
        dashboard_user,
        accessible_accounts,
        ad_account_ids=list(ad_account_scope.values_list('id_meta_ad_account', flat=True)),
        campaign_ids=list(campaign_scope.values_list('id_meta_campaign', flat=True)) if filters['campaign_ids'] else [],
        date_start=date_start,
        date_end=date_end,
    )
    metrics = _build_report_metrics(qs, live_metrics)

    previous_date_start, previous_date_end = _get_previous_period_range(date_start, date_end)
    previous_qs = CampaignInsightDaily.objects.filter(
        id_meta_campaign__id_meta_ad_account__in=accessible_accounts,
        created_at__gte=previous_date_start,
        created_at__lte=previous_date_end,
    )
    if filters['campaign_ids']:
        previous_qs = previous_qs.filter(id_meta_campaign__in=campaign_scope)
    else:
        previous_qs = previous_qs.filter(id_meta_campaign__id_meta_ad_account__in=ad_account_scope)

    previous_live_metrics = _fetch_live_report_metrics(
        dashboard_user,
        accessible_accounts,
        ad_account_ids=list(ad_account_scope.values_list('id_meta_ad_account', flat=True)),
        campaign_ids=list(campaign_scope.values_list('id_meta_campaign', flat=True)) if filters['campaign_ids'] else [],
        date_start=previous_date_start,
        date_end=previous_date_end,
        include_budget=False,
    )
    previous_metrics = _build_report_metrics(previous_qs, previous_live_metrics)
    metric_changes = _build_metric_changes(metrics, previous_metrics)

    return Response(
        {
            'date_start': date_start,
            'date_end': date_end,
            'previous_date_start': previous_date_start,
            'previous_date_end': previous_date_end,
            'filters': _serialize_meta_filter_values(filters),
            'metrics': metrics,
            'metric_changes': metric_changes,
        },
        status=status.HTTP_200_OK,
    )


def _statistics_queryset_context(dashboard_user, filters):
    context = _resolve_meta_filter_context(dashboard_user, filters)
    if filters['ad_ids']:
        return {
            'level': 'ad',
            'selected_entity_ids': filters['ad_ids'],
            'queryset': AdInsightDaily.objects.filter(id_meta_ad__in=context['ad_scope']),
        }
    if filters['adset_ids']:
        return {
            'level': 'adset',
            'selected_entity_ids': filters['adset_ids'],
            'queryset': AdSetInsightDaily.objects.filter(id_meta_adset__in=context['adset_scope']),
        }
    if filters['campaign_ids']:
        return {
            'level': 'campaign',
            'selected_entity_ids': filters['campaign_ids'],
            'queryset': CampaignInsightDaily.objects.filter(id_meta_campaign__in=context['campaign_scope']),
        }
    return {
        'level': 'ad_account',
        'selected_entity_ids': filters['ad_account_ids'],
        'queryset': CampaignInsightDaily.objects.filter(
            id_meta_campaign__id_meta_ad_account__in=context['ad_account_scope']
        ),
    }


def _clustering_queryset_context(dashboard_user, filters, entity_type):
    context = _resolve_meta_filter_context(dashboard_user, filters)
    if entity_type == 'lead':
        return {
            'level': 'lead',
            'queryset': None,
        }

    if entity_type == 'campaign':
        entity_scope = context['campaign_scope']
        if filters['ad_ids']:
            entity_scope = entity_scope.filter(adsets__ads__in=context['ad_scope'])
        elif filters['adset_ids']:
            entity_scope = entity_scope.filter(adsets__in=context['adset_scope'])
        return {
            'level': 'campaign',
            'queryset': CampaignInsightDaily.objects.filter(id_meta_campaign__in=entity_scope.distinct()),
        }

    if entity_type == 'adset':
        entity_scope = context['adset_scope']
        if filters['ad_ids']:
            entity_scope = entity_scope.filter(ads__in=context['ad_scope'])
        return {
            'level': 'adset',
            'queryset': AdSetInsightDaily.objects.filter(id_meta_adset__in=entity_scope.distinct()),
        }

    return {
        'level': 'ad',
        'queryset': AdInsightDaily.objects.filter(id_meta_ad__in=context['ad_scope'].distinct()),
    }


def _serialize_statistics_rows(queryset, level, date_start, date_end):
    queryset = queryset.filter(created_at__gte=date_start, created_at__lte=date_end).order_by('created_at')
    if level == 'ad':
        values = queryset.values(
            'id_meta_ad__id_meta_ad',
            'id_meta_ad__name',
            'created_at',
            'gasto_diario',
            'impressao_diaria',
            'alcance_diario',
            'quantidade_results_diaria',
            'quantidade_clicks_diaria',
        )
        id_field = 'id_meta_ad__id_meta_ad'
        name_field = 'id_meta_ad__name'
    elif level == 'adset':
        values = queryset.values(
            'id_meta_adset__id_meta_adset',
            'id_meta_adset__name',
            'created_at',
            'gasto_diario',
            'impressao_diaria',
            'alcance_diario',
            'quantidade_results_diaria',
            'quantidade_clicks_diaria',
        )
        id_field = 'id_meta_adset__id_meta_adset'
        name_field = 'id_meta_adset__name'
    elif level == 'campaign':
        values = queryset.values(
            'id_meta_campaign__id_meta_campaign',
            'id_meta_campaign__name',
            'created_at',
            'gasto_diario',
            'impressao_diaria',
            'alcance_diario',
            'quantidade_results_diaria',
            'quantidade_clicks_diaria',
        )
        id_field = 'id_meta_campaign__id_meta_campaign'
        name_field = 'id_meta_campaign__name'
    else:
        values = queryset.values(
            'id_meta_campaign__id_meta_ad_account__id_meta_ad_account',
            'id_meta_campaign__id_meta_ad_account__name',
            'created_at',
            'gasto_diario',
            'impressao_diaria',
            'alcance_diario',
            'quantidade_results_diaria',
            'quantidade_clicks_diaria',
        )
        id_field = 'id_meta_campaign__id_meta_ad_account__id_meta_ad_account'
        name_field = 'id_meta_campaign__id_meta_ad_account__name'

    return [
        {
            'entity_id': row[id_field],
            'entity_name': row[name_field],
            'date': row['created_at'],
            'spend': _to_float(row['gasto_diario']),
            'impressions': _to_int(row['impressao_diaria']),
            'reach': _to_int(row['alcance_diario']),
            'results': _to_int(row['quantidade_results_diaria']),
            'clicks': _to_int(row['quantidade_clicks_diaria']),
        }
        for row in values
    ]


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def statistics_analysis(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    date_start, date_end, date_error = _parse_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)

    filters = _get_meta_filter_values(request)
    try:
        statistics_context = _statistics_queryset_context(dashboard_user, filters)
    except ValueError as exc:
        return Response({'detail': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

    compare = str(request.query_params.get('compare', 'true')).strip().lower() not in {
        '0',
        'false',
        'no',
        'nao',
        'não',
    }
    level = statistics_context['level']
    queryset = statistics_context['queryset']
    current_rows = _serialize_statistics_rows(queryset, level, date_start, date_end)

    previous_date_start = None
    previous_date_end = None
    previous_rows = []
    if compare:
        previous_date_start, previous_date_end = _get_previous_period_range(date_start, date_end)
        previous_rows = _serialize_statistics_rows(
            queryset,
            level,
            previous_date_start,
            previous_date_end,
        )

    analysis = build_statistics_analysis(
        current_rows=current_rows,
        previous_rows=previous_rows,
        compare=compare,
        entity_type=level,
        selected_entity_ids=statistics_context['selected_entity_ids'],
    )
    analysis['segments']['breakdown'] = str(request.query_params.get('breakdown') or '').strip() or None

    return Response(
        {
            'meta': {
                'analysis_level': level,
                'date_start': date_start,
                'date_end': date_end,
                'compare': compare,
                'previous_date_start': previous_date_start,
                'previous_date_end': previous_date_end,
                'filters': _serialize_meta_filter_values(filters),
                'result_semantics': (
                    'Resultados refletem o objetivo configurado na campanha e podem representar leads, '
                    'mensagens ou outra ação.'
                ),
            },
            **analysis,
        },
        status=status.HTTP_200_OK,
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def statistics_time_series(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    date_start, date_end, date_error = _parse_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)

    metric = str(request.query_params.get('metric') or 'cpl').strip().lower()
    if metric not in TIME_SERIES_METRIC_CONFIG:
        valid_metrics = ', '.join(TIME_SERIES_METRIC_CONFIG)
        return Response(
            {'detail': f'Métrica inválida. Use uma destas opções: {valid_metrics}.'},
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        forecast_days = int(request.query_params.get('forecast_days') or 7)
    except (TypeError, ValueError):
        forecast_days = 0
    if forecast_days < 1 or forecast_days > 30:
        return Response(
            {'detail': 'forecast_days deve ser um número inteiro entre 1 e 30.'},
            status=status.HTTP_400_BAD_REQUEST,
        )

    goal_leads = None
    raw_goal_leads = str(request.query_params.get('goal_leads') or '').strip()
    if raw_goal_leads:
        try:
            goal_leads = float(Decimal(raw_goal_leads))
        except (InvalidOperation, TypeError, ValueError):
            goal_leads = 0
        if goal_leads <= 0:
            return Response(
                {'detail': 'goal_leads deve ser um número positivo.'},
                status=status.HTTP_400_BAD_REQUEST,
            )

    filters = _get_meta_filter_values(request)
    try:
        statistics_context = _statistics_queryset_context(dashboard_user, filters)
    except ValueError as exc:
        return Response({'detail': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

    rows = _serialize_statistics_rows(
        statistics_context['queryset'],
        statistics_context['level'],
        date_start,
        date_end,
    )
    analysis = build_time_series_analysis(
        rows=rows,
        date_start=date_start,
        date_end=date_end,
        metric=metric,
        forecast_days=forecast_days,
        goal_leads=goal_leads,
    )
    analysis['meta'].update(
        {
            'analysis_level': statistics_context['level'],
            'filters': _serialize_meta_filter_values(filters),
        }
    )
    return Response(analysis, status=status.HTTP_200_OK)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def statistics_clustering(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    date_start, date_end, date_error = _parse_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)

    entity_type = str(request.query_params.get('entity_type') or 'campaign').strip().lower()
    if entity_type not in {'campaign', 'adset', 'ad', 'lead'}:
        return Response(
            {'detail': 'entity_type invalido. Use campaign, adset, ad ou lead.'},
            status=status.HTTP_400_BAD_REQUEST,
        )

    algorithm = str(request.query_params.get('algorithm') or 'kmeans').strip().lower()
    if algorithm != 'kmeans':
        return Response(
            {'detail': 'Nesta versão, apenas algorithm=kmeans está disponível.'},
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        requested_clusters = int(request.query_params.get('clusters') or 3)
    except (TypeError, ValueError):
        requested_clusters = 0
    if requested_clusters not in {2, 3, 4, 5}:
        return Response(
            {'detail': 'clusters invalido. Use 2, 3, 4 ou 5.'},
            status=status.HTTP_400_BAD_REQUEST,
        )

    normalize = str(request.query_params.get('normalize', 'true')).strip().lower() not in {
        '0',
        'false',
        'no',
        'nao',
        'não',
    }
    filters = _get_meta_filter_values(request)
    try:
        clustering_context = _clustering_queryset_context(
            dashboard_user,
            filters,
            entity_type,
        )
    except ValueError as exc:
        return Response({'detail': str(exc)}, status=status.HTTP_400_BAD_REQUEST)

    rows = []
    if clustering_context['queryset'] is not None:
        rows = _serialize_statistics_rows(
            clustering_context['queryset'],
            clustering_context['level'],
            date_start,
            date_end,
        )

    analysis = build_clustering_analysis(
        rows=rows,
        entity_type=entity_type,
        requested_clusters=requested_clusters,
        normalize=normalize,
    )
    return Response(
        {
            'meta': {
                'entity_type': entity_type,
                'algorithm': algorithm,
                'requested_clusters': requested_clusters,
                'date_start': date_start,
                'date_end': date_end,
                'filters': _serialize_meta_filter_values(filters),
                'result_semantics': (
                    'Resultados refletem o objetivo configurado na campanha e não representam '
                    'necessariamente vendas ou leads qualificados.'
                ),
            },
            **analysis,
        },
        status=status.HTTP_200_OK,
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def meta_specific_insights(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    date_start, date_end, date_error = _parse_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)

    raw_filters = _get_meta_filter_values(request)
    filters = {
        'ad_account_ids': raw_filters['ad_account_ids'],
        'campaign_ids': raw_filters['campaign_ids'],
        'adset_ids': raw_filters['adset_ids'],
        'ad_ids': [],
    }
    try:
        level, qs = _build_meta_specific_ad_queryset(dashboard_user, filters)
    except ValueError as exc:
        return Response({'detail': str(exc)}, status=status.HTTP_400_BAD_REQUEST)
    qs = qs.filter(created_at__gte=date_start, created_at__lte=date_end)

    daily_rows = (
        qs.values('created_at')
        .annotate(
            spend_total=Sum('gasto_diario'),
            results_total=Sum('quantidade_results_diaria'),
        )
        .order_by('created_at')
    )
    rows_by_ad = (
        qs.values('id_meta_ad__id_meta_ad', 'id_meta_ad__name')
        .annotate(
            spend_total=Sum('gasto_diario'),
            results_total=Sum('quantidade_results_diaria'),
        )
        .order_by('id_meta_ad__name', 'id_meta_ad__id_meta_ad')
    )
    timeseries_by_ad_rows = (
        qs.values('id_meta_ad__id_meta_ad', 'id_meta_ad__name', 'created_at')
        .annotate(
            spend_total=Sum('gasto_diario'),
            results_total=Sum('quantidade_results_diaria'),
        )
        .order_by('id_meta_ad__name', 'id_meta_ad__id_meta_ad', 'created_at')
    )

    payload = {
        'level': level,
        'date_start': date_start,
        'date_end': date_end,
        'filters': _serialize_meta_filter_values(filters),
        'timeseries_daily': [
            {
                'date': row['created_at'],
                'spend': round(_to_float(row['spend_total']), 4),
                'results': _to_int(row['results_total']),
            }
            for row in daily_rows
        ],
        'timeseries_by_ad': [],
        'rows_by_ad': [],
    }

    series_by_ad_map = {}
    for row in timeseries_by_ad_rows:
        ad_id = row['id_meta_ad__id_meta_ad']
        current = series_by_ad_map.get(ad_id)
        if current is None:
            current = {
                'ad_id': ad_id,
                'ad_name': row['id_meta_ad__name'] or ad_id,
                'points': [],
            }
            series_by_ad_map[ad_id] = current
        current['points'].append(
            {
                'date': row['created_at'],
                'spend': round(_to_float(row['spend_total']), 4),
                'results': _to_int(row['results_total']),
            }
        )

    payload['timeseries_by_ad'] = list(series_by_ad_map.values())

    for row in rows_by_ad:
        spend_total = round(_to_float(row['spend_total']), 4)
        results_total = _to_int(row['results_total'])
        payload['rows_by_ad'].append(
            {
                'ad_id': row['id_meta_ad__id_meta_ad'],
                'ad_name': row['id_meta_ad__name'] or row['id_meta_ad__id_meta_ad'],
                'results': results_total,
                'spend': spend_total,
                'cpr': round(spend_total / results_total, 4) if results_total > 0 else None,
            }
        )

    serializer = MetaSpecificInsightsSerializer(payload)
    return Response(serializer.data, status=status.HTTP_200_OK)


def _instagram_accounts_queryset(dashboard_user: DashboardUser):
    return InstagramAccount.objects.filter(id_page__dashboard_user_id=dashboard_user)


def _instagram_daily_insights_queryset(accounts_qs, date_start, date_end):
    return InstagramAccountInsightDaily.objects.filter(
        id_meta_instagram__in=accounts_qs,
        created_at__gte=date_start,
        created_at__lte=date_end,
    )


def _iter_dates(date_start, date_end):
    current = date_start
    while current <= date_end:
        yield current
        current += timedelta(days=1)


def _current_instagram_followers_total(accounts_qs) -> int:
    # Source of truth: follower_count extracted from Meta and persisted on InstagramAccount.
    accounts = list(accounts_qs.only('id', 'follower_count'))
    if not accounts:
        return 0

    followers_by_account = {}
    accounts_without_snapshot = []
    for account in accounts:
        if account.follower_count is None:
            accounts_without_snapshot.append(account.id)
            continue
        followers_by_account[account.id] = _to_int(account.follower_count)

    # Fallback for accounts that still don't have snapshot follower_count populated.
    if accounts_without_snapshot:
        rows = (
            InstagramAccountInsightDaily.objects.filter(id_meta_instagram_id__in=accounts_without_snapshot)
            .order_by('id_meta_instagram_id', '-created_at', '-id')
            .values('id_meta_instagram_id', 'follower_count')
        )
        latest_daily_by_account = {}
        for row in rows:
            account_pk = row['id_meta_instagram_id']
            if account_pk in latest_daily_by_account:
                continue
            if row['follower_count'] is None:
                continue
            latest_daily_by_account[account_pk] = _to_int(row['follower_count'])

        accounts_to_update = []
        for account in accounts:
            if account.id not in latest_daily_by_account:
                continue
            follower_count = latest_daily_by_account[account.id]
            followers_by_account[account.id] = follower_count
            if account.follower_count is None:
                account.follower_count = follower_count
                accounts_to_update.append(account)

        if accounts_to_update:
            InstagramAccount.objects.bulk_update(accounts_to_update, ['follower_count'])

    return sum(followers_by_account.values())


def _build_instagram_followers_timeseries(accounts_qs, date_start, date_end):
    dates = list(_iter_dates(date_start, date_end))
    if not dates:
        return {}

    history_end = max(date_end, timezone.localdate())
    rows = (
        InstagramAccountInsightDaily.objects.filter(
            id_meta_instagram__in=accounts_qs,
            created_at__gte=date_start,
            created_at__lte=history_end,
        )
        .values('id_meta_instagram_id', 'created_at', 'follower_count', 'follows_and_unfollows')
        .order_by('id_meta_instagram_id', 'created_at')
    )

    rows_by_account = {}
    for row in rows:
        rows_by_account.setdefault(row['id_meta_instagram_id'], []).append(row)

    totals_by_date = {current_date: 0 for current_date in dates}
    has_followers = False

    for account in accounts_qs.only('id', 'follower_count'):
        account_rows = rows_by_account.get(account.id, [])
        delta_by_date = {row['created_at']: _to_int(row['follows_and_unfollows']) for row in account_rows}
        direct_followers_by_date = {
            row['created_at']: _to_int(row['follower_count'])
            for row in account_rows
            if row['follower_count'] is not None
        }

        if direct_followers_by_date:
            anchor_date = max(direct_followers_by_date.keys())
            anchor_count = direct_followers_by_date[anchor_date]
        elif account.follower_count is not None:
            anchor_date = date_end
            anchor_count = _to_int(account.follower_count)
        else:
            continue

        account_series = {anchor_date: anchor_count}

        current_value = anchor_count
        current_date = anchor_date + timedelta(days=1)
        while current_date <= history_end:
            current_value += delta_by_date.get(current_date, 0)
            account_series[current_date] = current_value
            current_date += timedelta(days=1)

        current_value = anchor_count
        current_date = anchor_date - timedelta(days=1)
        while current_date >= date_start:
            next_date = current_date + timedelta(days=1)
            current_value -= delta_by_date.get(next_date, 0)
            account_series[current_date] = current_value
            current_date -= timedelta(days=1)

        for direct_date, direct_value in direct_followers_by_date.items():
            if date_start <= direct_date <= history_end:
                account_series[direct_date] = direct_value

        for current_date in dates:
            follower_value = account_series.get(current_date)
            if follower_value is None:
                continue
            totals_by_date[current_date] += follower_value
            has_followers = True

    if not has_followers:
        return {}
    return totals_by_date


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def instagram_accounts(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    rows = (
        _instagram_accounts_queryset(dashboard_user)
        .select_related('id_page')
        .only('id_meta_instagram', 'name', 'id_page__id_meta_page')
        .order_by('name', 'id_meta_instagram')
    )

    return Response(
        {
            'accounts': [
                {
                    'id_meta_instagram': row.id_meta_instagram,
                    'name': row.name,
                    'id_meta_page': row.id_page.id_meta_page,
                }
                for row in rows
            ]
        },
        status=status.HTTP_200_OK,
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def instagram_kpis(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    date_start, date_end, date_error = _parse_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)

    instagram_account_id = str(request.query_params.get('instagram_account_id') or '').strip()
    accounts_qs = _instagram_accounts_queryset(dashboard_user)
    if instagram_account_id:
        accounts_qs = accounts_qs.filter(id_meta_instagram=instagram_account_id)

    daily_qs = _instagram_daily_insights_queryset(accounts_qs, date_start, date_end)
    daily_totals = daily_qs.aggregate(accounts_reached_total=Sum('accounts_reached'))
    snapshot_totals = accounts_qs.aggregate(accounts_reached_total=Sum('accounts_reached'))

    daily_reach = _to_int(daily_totals['accounts_reached_total'])
    snapshot_reach = _to_int(snapshot_totals['accounts_reached_total'])
    alcance = daily_reach if daily_reach > 0 else snapshot_reach
    seguidores_atuais = _current_instagram_followers_total(accounts_qs)

    return Response(
        {
            'instagram_account_id': instagram_account_id or None,
            'date_start': date_start,
            'date_end': date_end,
            'kpis': {
                'alcance': alcance,
                'seguidores_atuais': seguidores_atuais,
            },
        },
        status=status.HTTP_200_OK,
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def instagram_timeseries(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    date_start, date_end, date_error = _parse_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)

    instagram_account_id = str(request.query_params.get('instagram_account_id') or '').strip()
    accounts_qs = _instagram_accounts_queryset(dashboard_user)
    if instagram_account_id:
        accounts_qs = accounts_qs.filter(id_meta_instagram=instagram_account_id)

    rows = (
        _instagram_daily_insights_queryset(accounts_qs, date_start, date_end)
        .values('created_at')
        .annotate(
            impressions_total=Sum('impressions'),
            reach_total=Sum('accounts_reached'),
        )
        .order_by('created_at')
    )
    by_date = {row['created_at']: row for row in rows}
    followers_by_date = _build_instagram_followers_timeseries(accounts_qs, date_start, date_end)

    timeseries = [
        {
            'date': current_date,
            'impressions': _to_int((by_date.get(current_date) or {}).get('impressions_total')),
            'reach': _to_int((by_date.get(current_date) or {}).get('reach_total')),
            'follower_count': (
                None if current_date not in followers_by_date else _to_int(followers_by_date.get(current_date))
            ),
        }
        for current_date in _iter_dates(date_start, date_end)
    ]

    return Response(
        {
            'instagram_account_id': instagram_account_id or None,
            'date_start': date_start,
            'date_end': date_end,
            'timeseries': timeseries,
        },
        status=status.HTTP_200_OK,
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def instagram_media_table(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    date_start, date_end, date_error = _parse_date_range(request)
    if date_error:
        return Response({'detail': date_error}, status=status.HTTP_400_BAD_REQUEST)

    instagram_account_id = str(request.query_params.get('instagram_account_id') or '').strip()
    ordering_raw = str(request.query_params.get('ordering') or request.query_params.get('order_by') or '-date').strip()

    try:
        limit = int(request.query_params.get('limit', 100))
    except (TypeError, ValueError):
        limit = 100
    try:
        offset = int(request.query_params.get('offset', 0))
    except (TypeError, ValueError):
        offset = 0
    limit = max(1, min(limit, 500))
    offset = max(0, offset)

    accounts_qs = _instagram_accounts_queryset(dashboard_user)
    if instagram_account_id:
        accounts_qs = accounts_qs.filter(id_meta_instagram=instagram_account_id)

    qs = MediaInstagram.objects.filter(id_meta_instagram__in=accounts_qs)
    qs = qs.filter(timestamp__date__gte=date_start, timestamp__date__lte=date_end)

    order_map = {
        'date': 'timestamp',
        'tipo': 'media_type',
        'media_type': 'media_type',
        'caption': 'caption',
        'reach': 'reach',
        'views': 'views',
        'likes': 'likes',
        'comentarios': 'comments',
        'comments': 'comments',
        'saved': 'saved',
        'shares': 'shares',
        'plays': 'plays',
        'link': 'permalink',
    }
    descending = ordering_raw.startswith('-')
    order_key = ordering_raw[1:] if descending else ordering_raw
    order_field = order_map.get(order_key, 'timestamp')
    ordering = f'-{order_field}' if descending else order_field

    total = qs.count()
    rows = qs.order_by(ordering, '-id')[offset : offset + limit]

    data = [
        {
            'id_meta_media': row.id_meta_media,
            'id_meta_instagram': row.id_meta_instagram.id_meta_instagram,
            'date': row.timestamp,
            'tipo': row.media_type,
            'caption': row.caption,
            'reach': _to_int(row.reach),
            'views': _to_int(row.views),
            'likes': _to_int(row.likes),
            'comments': _to_int(row.comments),
            'saved': _to_int(row.saved),
            'shares': _to_int(row.shares),
            'plays': _to_int(row.plays),
            'link': row.permalink,
            'media_url': row.media_url,
        }
        for row in rows.select_related('id_meta_instagram').only(
            'id_meta_media',
            'id_meta_instagram__id_meta_instagram',
            'timestamp',
            'media_type',
            'caption',
            'reach',
            'views',
            'likes',
            'comments',
            'saved',
            'shares',
            'plays',
            'permalink',
            'media_url',
        )
    ]

    return Response(
        {
            'instagram_account_id': instagram_account_id or None,
            'date_start': date_start,
            'date_end': date_end,
            'ordering': ordering_raw,
            'offset': offset,
            'limit': limit,
            'total': total,
            'rows': data,
        },
        status=status.HTTP_200_OK,
    )
