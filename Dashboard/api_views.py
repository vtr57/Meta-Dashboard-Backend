from datetime import timedelta
import logging
import re
import threading
from decimal import Decimal
from typing import Optional

import pandas as pd
from scipy.stats import pearsonr
from django.db import transaction
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
    Campaign,
    CampaignInsightDaily,
    DashboardUser,
    InstagramAccount,
    MediaInstagram,
    SyncLog,
    SyncRun,
)
from Dashboard.services.meta_sync_orchestrator import MetaSyncOrchestrator
from loginFacebook.services import (
    MetaTokenExchangeError,
    exchange_short_token_for_long_token,
)


logger = logging.getLogger(__name__)
OWNER_RE = re.compile(r'user_id=(\d+)')


def _run_sync_in_background(
    sync_run_id: int,
    dashboard_user_id: int,
    sync_scope: str = 'all',
    insights_days_override: Optional[int] = None,
) -> None:
    try:
        MetaSyncOrchestrator(
            sync_run_id=sync_run_id,
            dashboard_user_id=dashboard_user_id,
            sync_scope=sync_scope,
            insights_days_override=insights_days_override,
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


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def meta_connect(request):
    id_meta_user = str(request.data.get('id_meta_user') or '').strip()
    short_token = str(request.data.get('short_token') or '').strip()

    if not id_meta_user or not short_token:
        return Response(
            {'detail': 'id_meta_user e short_token sao obrigatorios.'},
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        exchange = exchange_short_token_for_long_token(short_token=short_token)
    except MetaTokenExchangeError as exc:
        return Response({'detail': exc.detail}, status=exc.status_code)

    long_token = exchange['long_token']
    expired_at = exchange['expired_at']
    expiration_source = exchange['expiration_source']
    logger.info(
        '[meta_connect] expiration_source=%s; expired_at=%s',
        expiration_source,
        expired_at.isoformat(),
    )

    with transaction.atomic():
        already_linked = (
            DashboardUser.objects.select_for_update()
            .filter(id_meta_user=id_meta_user)
            .exclude(user=request.user)
            .exists()
        )
        if already_linked:
            return Response(
                {'detail': 'id_meta_user ja conectado a outro usuario do sistema.'},
                status=status.HTTP_409_CONFLICT,
            )

        dashboard_user, _ = DashboardUser.objects.select_for_update().get_or_create(
            user=request.user,
            defaults={
                'id_meta_user': id_meta_user,
                'long_access_token': long_token,
                'expired_at': expired_at,
            },
        )
        dashboard_user.id_meta_user = id_meta_user
        dashboard_user.long_access_token = long_token
        dashboard_user.expired_at = expired_at
        dashboard_user.save(update_fields=['id_meta_user', 'long_access_token', 'expired_at'])

    return Response(
        {
            'detail': 'Conexao com Meta concluida.',
            'id_meta_user': dashboard_user.id_meta_user,
            'expired_at': dashboard_user.expired_at,
            'has_valid_long_token': dashboard_user.has_valid_long_token(),
            'sync_requires_reconnect': False,
        },
        status=status.HTTP_200_OK,
    )


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
    return _start_sync(request, sync_scope='all')


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def meta_sync_start_meta(request):
    return _start_sync(request, sync_scope='meta')


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def meta_sync_start_instagram(request):
    return _start_sync(request, sync_scope='instagram')


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def meta_sync_start_insights_7d(request):
    return _start_sync(request, sync_scope='all', insights_days_override=7)


def _start_sync(request, sync_scope: str, insights_days_override: Optional[int] = None):
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
        mensagem=f'user_id={request.user.id};dashboard_user_id={dashboard_user.id}',
    )
    SyncLog.objects.create(
        sync_run=sync_run,
        entidade='sync',
        mensagem=(
            f'Sincronizacao enfileirada. Escopo={sync_scope}.'
            + (
                f' Janela de insights: ultimos {insights_days_override} dias.'
                if insights_days_override is not None
                else ''
            )
        ),
    )

    thread = threading.Thread(
        target=_run_sync_in_background,
        args=(sync_run.id, dashboard_user.id, sync_scope, insights_days_override),
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
    start_raw = request.query_params.get('date_start') or request.query_params.get('start_date')
    end_raw = request.query_params.get('date_end') or request.query_params.get('end_date')
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


def _get_dashboard_user_or_error(request):
    dashboard_user = DashboardUser.objects.filter(user=request.user).first()
    if dashboard_user is None:
        return None, Response(
            {'detail': 'Usuario nao conectado na Meta.'},
            status=status.HTTP_400_BAD_REQUEST,
        )
    return dashboard_user, None


def _get_meta_filter_values(request):
    return {
        'ad_account_id': str(request.query_params.get('ad_account_id') or '').strip(),
        'campaign_id': str(request.query_params.get('campaign_id') or '').strip(),
        'adset_id': str(request.query_params.get('adset_id') or '').strip(),
        'ad_id': str(request.query_params.get('ad_id') or '').strip(),
    }


def _build_meta_insight_queryset(dashboard_user: DashboardUser, filters: dict):
    ad_account_id = filters['ad_account_id']
    campaign_id = filters['campaign_id']
    adset_id = filters['adset_id']
    ad_id = filters['ad_id']

    if ad_id:
        level = 'ad'
        qs = AdInsightDaily.objects.filter(
            id_meta_ad__id_meta_ad=ad_id,
            id_meta_ad__id_meta_adset__id_meta_campaign__id_meta_ad_account__id_dashboard_user=dashboard_user,
        )
        if ad_account_id:
            qs = qs.filter(
                id_meta_ad__id_meta_adset__id_meta_campaign__id_meta_ad_account__id_meta_ad_account=ad_account_id
            )
        if campaign_id:
            qs = qs.filter(id_meta_ad__id_meta_adset__id_meta_campaign__id_meta_campaign=campaign_id)
        if adset_id:
            qs = qs.filter(id_meta_ad__id_meta_adset__id_meta_adset=adset_id)
        return level, qs

    if adset_id:
        level = 'adset'
        qs = AdSetInsightDaily.objects.filter(
            id_meta_adset__id_meta_adset=adset_id,
            id_meta_adset__id_meta_campaign__id_meta_ad_account__id_dashboard_user=dashboard_user,
        )
        if ad_account_id:
            qs = qs.filter(id_meta_adset__id_meta_campaign__id_meta_ad_account__id_meta_ad_account=ad_account_id)
        if campaign_id:
            qs = qs.filter(id_meta_adset__id_meta_campaign__id_meta_campaign=campaign_id)
        return level, qs

    if campaign_id:
        level = 'campaign'
        qs = CampaignInsightDaily.objects.filter(
            id_meta_campaign__id_meta_campaign=campaign_id,
            id_meta_campaign__id_meta_ad_account__id_dashboard_user=dashboard_user,
        )
        if ad_account_id:
            qs = qs.filter(id_meta_campaign__id_meta_ad_account__id_meta_ad_account=ad_account_id)
        return level, qs

    level = 'ad_account'
    qs = CampaignInsightDaily.objects.filter(id_meta_campaign__id_meta_ad_account__id_dashboard_user=dashboard_user)
    if ad_account_id:
        qs = qs.filter(id_meta_campaign__id_meta_ad_account__id_meta_ad_account=ad_account_id)
    return level, qs


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def meta_filters(request):
    dashboard_user, error_response = _get_dashboard_user_or_error(request)
    if error_response:
        return error_response

    ad_account_id = str(request.query_params.get('ad_account_id') or '').strip()
    campaign_id = str(request.query_params.get('campaign_id') or '').strip()
    adset_id = str(request.query_params.get('adset_id') or '').strip()

    ad_accounts_qs = AdAccount.objects.filter(id_dashboard_user=dashboard_user).order_by('name', 'id_meta_ad_account')
    campaigns_qs = Campaign.objects.filter(id_meta_ad_account__in=ad_accounts_qs)
    if ad_account_id:
        campaigns_qs = campaigns_qs.filter(id_meta_ad_account__id_meta_ad_account=ad_account_id)
    campaigns_qs = campaigns_qs.order_by('name', 'id_meta_campaign')

    adsets_qs = AdSet.objects.filter(id_meta_campaign__in=campaigns_qs)
    if campaign_id:
        adsets_qs = adsets_qs.filter(id_meta_campaign__id_meta_campaign=campaign_id)
    adsets_qs = adsets_qs.order_by('name', 'id_meta_adset')

    ads_qs = Ad.objects.filter(id_meta_adset__in=adsets_qs)
    if adset_id:
        ads_qs = ads_qs.filter(id_meta_adset__id_meta_adset=adset_id)
    ads_qs = ads_qs.order_by('name', 'id_meta_ad')

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
                }
                for row in campaigns_qs.select_related('id_meta_ad_account').only(
                    'id_meta_campaign',
                    'name',
                    'id_meta_ad_account__id_meta_ad_account',
                )
            ],
            'adsets': [
                {
                    'id_meta_adset': row.id_meta_adset,
                    'id_meta_campaign': row.id_meta_campaign.id_meta_campaign,
                    'name': row.name,
                }
                for row in adsets_qs.select_related('id_meta_campaign').only(
                    'id_meta_adset',
                    'name',
                    'id_meta_campaign__id_meta_campaign',
                )
            ],
            'ads': [
                {
                    'id_meta_ad': row.id_meta_ad,
                    'id_meta_adset': row.id_meta_adset.id_meta_adset,
                    'name': row.name,
                }
                for row in ads_qs.select_related('id_meta_adset').only(
                    'id_meta_ad',
                    'name',
                    'id_meta_adset__id_meta_adset',
                )
            ],
        },
        status=status.HTTP_200_OK,
    )


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
    level, qs = _build_meta_insight_queryset(dashboard_user, filters)
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
            'filters': filters,
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
    level, qs = _build_meta_insight_queryset(dashboard_user, filters)
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
            'filters': filters,
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


def _instagram_accounts_queryset(dashboard_user: DashboardUser):
    return InstagramAccount.objects.filter(id_page__dashboard_user_id=dashboard_user)


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

    media_qs = MediaInstagram.objects.filter(id_meta_instagram__in=accounts_qs)
    media_qs = media_qs.filter(timestamp__date__gte=date_start, timestamp__date__lte=date_end)

    media_totals = media_qs.aggregate(
        reach_total=Sum('reach'),
        views_total=Sum('views'),
        likes_total=Sum('likes'),
        comments_total=Sum('comments'),
        saved_total=Sum('saved'),
        shares_total=Sum('shares'),
        plays_total=Sum('plays'),
    )
    account_totals = accounts_qs.aggregate(
        accounts_reached_total=Sum('accounts_reached'),
        impressions_total=Sum('impressions'),
        profile_views_total=Sum('profile_views'),
        accounts_engaged_total=Sum('accounts_engaged'),
        follower_count_total=Sum('follower_count'),
        follows_and_unfollows_total=Sum('follows_and_unfollows'),
    )

    media_reach = _to_int(media_totals['reach_total'])
    media_views = _to_int(media_totals['views_total'])
    account_reach = _to_int(account_totals['accounts_reached_total'])
    account_impressions = _to_int(account_totals['impressions_total'])

    alcance = media_reach if media_reach > 0 else account_reach
    impressoes = media_views if media_views > 0 else account_impressions

    return Response(
        {
            'instagram_account_id': instagram_account_id or None,
            'date_start': date_start,
            'date_end': date_end,
            'kpis': {
                'alcance': alcance,
                'impressoes': impressoes,
                'curtidas': _to_int(media_totals['likes_total']),
                'comentarios': _to_int(media_totals['comments_total']),
                'salvos': _to_int(media_totals['saved_total']),
                'compartilhamentos': _to_int(media_totals['shares_total']),
                'plays': _to_int(media_totals['plays_total']),
                'profile_views': _to_int(account_totals['profile_views_total']),
                'accounts_engaged': _to_int(account_totals['accounts_engaged_total']),
                'follower_count': _to_int(account_totals['follower_count_total']),
                'follows_and_unfollows': _to_int(account_totals['follows_and_unfollows_total']),
            },
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
