import calendar
import json
import logging
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone as dt_timezone
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse

from django.db import close_old_connections
from django.utils import timezone
from django.utils.dateparse import parse_date, parse_datetime

from Dashboard.models import (
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
    SyncLog,
    SyncRun,
)
from Dashboard.services.meta_client import MetaClientError, MetaGraphClient


logger = logging.getLogger(__name__)


class MetaSyncOrchestrator:
    SCOPE_ALL = 'all'
    SCOPE_META = 'meta'
    SCOPE_INSTAGRAM = 'instagram'
    VALID_SCOPES = {SCOPE_ALL, SCOPE_META, SCOPE_INSTAGRAM}

    def __init__(
        self,
        sync_run_id: int,
        dashboard_user_id: int,
        sync_scope: str = SCOPE_ALL,
        insights_days_override: Optional[int] = None,
    ) -> None:
        self.sync_run_id = sync_run_id
        self.dashboard_user_id = dashboard_user_id
        self.sync_scope = str(sync_scope or self.SCOPE_ALL).strip().lower()
        self.insights_days_override = self._normalize_insights_days_override(insights_days_override)
        self.sync_run: Optional[SyncRun] = None
        self.dashboard_user: Optional[DashboardUser] = None
        self.client: Optional[MetaGraphClient] = None

    def run(self) -> None:
        close_old_connections()
        try:
            self.sync_run = SyncRun.objects.get(id=self.sync_run_id)
            self.dashboard_user = DashboardUser.objects.get(id=self.dashboard_user_id)

            if not self.dashboard_user.has_valid_long_token():
                self._log('sync', 'Token invalido/expirado. Sincronizacao cancelada.')
                self._finish(SyncRun.Status.FAILED)
                return

            self.client = MetaGraphClient(
                access_token=self.dashboard_user.long_access_token,
                sync_run=self.sync_run,
                request_pause_seconds=0.6,
                max_retries=5,
                batch_size=50,
            )

            self._set_status(SyncRun.Status.RUNNING)
            if self.sync_scope not in self.VALID_SCOPES:
                self._log('sync', f'Escopo de sincronizacao invalido: {self.sync_scope}.')
                self._finish(SyncRun.Status.FAILED)
                return

            self._log('sync', f'Sincronizacao iniciada. Escopo={self.sync_scope}.')

            since, until = self._build_date_window()
            if self.insights_days_override is not None:
                self._log(
                    'sync',
                    (
                        f'Janela de extracao: {since.isoformat()} ate {until.isoformat()} '
                        f'(ultimos {self.insights_days_override} dias).'
                    ),
                )
            else:
                self._log('sync', f'Janela de extracao: {since.isoformat()} ate {until.isoformat()} (max 24 meses).')

            if self.sync_scope in {self.SCOPE_ALL, self.SCOPE_META}:
                self._run_stage('Ad Accounts', self.sync_ad_accounts)
                self._run_stage('Campaigns', self.sync_campaigns)
                self._run_stage('AdSets', self.sync_adsets)
                self._run_stage('Ads', self.sync_ads)
                self._run_stage('Ad Insights (somente anuncio)', self.sync_ad_insights, since, until)

            if self.sync_scope in {self.SCOPE_ALL, self.SCOPE_INSTAGRAM}:
                page_map = self._run_stage('Facebook Pages', self.sync_facebook_pages)
                self._run_stage(
                    'Instagram Business + insights da conta',
                    self.sync_instagram_accounts_and_insights,
                    page_map,
                    since,
                    until,
                )
                self._run_stage('Midias + insights das midias', self.sync_media_and_insights, since, until)

            self._log('sync', 'Sincronizacao concluida com sucesso.')
            self._finish(SyncRun.Status.SUCCESS)
        except Exception as exc:
            logger.exception('Meta sync failed.')
            self._log('sync', f'Erro na sincronizacao: {exc}')
            if self.sync_run is not None:
                self._finish(SyncRun.Status.FAILED)
        finally:
            close_old_connections()

    def sync_ad_accounts(self) -> Dict:
        assert self.client and self.dashboard_user
        total = 0
        for item in self.client.paginate(
            'me/adaccounts',
            params={'fields': 'id,name', 'limit': 100},
            entity='ad_accounts',
        ):
            account_id = str(item.get('id') or '').strip()
            if not account_id:
                continue
            AdAccount.objects.update_or_create(
                id_meta_ad_account=account_id,
                defaults={
                    'name': (item.get('name') or '').strip()[:255],
                    'id_dashboard_user': self.dashboard_user,
                },
            )
            total += 1
        return {'ad_accounts_upserted': total}

    def sync_campaigns(self) -> Dict:
        assert self.client and self.dashboard_user
        total = 0
        errors = 0
        accounts = AdAccount.objects.filter(id_dashboard_user=self.dashboard_user).only('id', 'id_meta_ad_account')
        batch_requests = [
            {
                'relative_url': self._to_batch_relative_url(
                    self._ad_account_edge_path(account.id_meta_ad_account, 'campaigns'),
                    params={
                        'fields': 'id,name,status,created_time,effective_status',
                        'limit': 200,
                    },
                ),
                'account_pk': account.id,
                'account_meta_id': account.id_meta_ad_account,
            }
            for account in accounts
        ]
        if not batch_requests:
            return {'campaigns_upserted': 0, 'campaigns_batch_errors': 0}

        self._log('campaigns', f'Extraindo campaigns em batch para {len(batch_requests)} contas (chunk=50).')
        for request_meta, result in self._iter_batch_paginated_requests(
            batch_requests,
            entity='campaigns_batch',
            batch_size=50,
        ):
            if result['status_code'] >= 400:
                errors += 1
                self._log(
                    'campaigns',
                    (
                        f'Falha no batch de campaigns para conta {request_meta["account_meta_id"]}: '
                        f'status={result["status_code"]}.'
                    ),
                )
                continue
            body = result.get('body')
            if not isinstance(body, dict):
                continue
            for item in body.get('data') or []:
                campaign_id = str(item.get('id') or '').strip()
                if not campaign_id:
                    continue
                Campaign.objects.update_or_create(
                    id_meta_campaign=campaign_id,
                    defaults={
                        'id_meta_ad_account_id': request_meta['account_pk'],
                        'name': (item.get('name') or '').strip()[:255],
                        'status': (item.get('status') or '').strip()[:100],
                        'created_time': self._parse_meta_datetime(item.get('created_time')),
                        'effective_status': (item.get('effective_status') or '').strip()[:100],
                    },
                )
                total += 1
        return {'campaigns_upserted': total, 'campaigns_batch_errors': errors}

    def sync_adsets(self) -> Dict:
        assert self.client and self.dashboard_user
        total = 0
        skipped = 0
        errors = 0
        accounts = AdAccount.objects.filter(id_dashboard_user=self.dashboard_user).only('id_meta_ad_account')
        campaign_map = {
            c.id_meta_campaign: c.id
            for c in Campaign.objects.filter(id_meta_ad_account__id_dashboard_user=self.dashboard_user).only(
                'id', 'id_meta_campaign'
            )
        }
        batch_requests = [
            {
                'relative_url': self._to_batch_relative_url(
                    self._ad_account_edge_path(account.id_meta_ad_account, 'adsets'),
                    params={
                        'fields': 'id,campaign_id,name,status,created_time,effective_status',
                        'limit': 200,
                    },
                ),
                'account_meta_id': account.id_meta_ad_account,
            }
            for account in accounts
        ]
        if not batch_requests:
            return {
                'adsets_upserted': 0,
                'adsets_skipped_missing_campaign': 0,
                'adsets_batch_errors': 0,
            }

        self._log('adsets', f'Extraindo adsets em batch para {len(batch_requests)} contas (chunk=50).')
        for request_meta, result in self._iter_batch_paginated_requests(
            batch_requests,
            entity='adsets_batch',
            batch_size=50,
        ):
            if result['status_code'] >= 400:
                errors += 1
                self._log(
                    'adsets',
                    (
                        f'Falha no batch de adsets para conta {request_meta["account_meta_id"]}: '
                        f'status={result["status_code"]}.'
                    ),
                )
                continue
            body = result.get('body')
            if not isinstance(body, dict):
                continue
            for item in body.get('data') or []:
                adset_id = str(item.get('id') or '').strip()
                campaign_id = str(item.get('campaign_id') or '').strip()
                campaign_pk = campaign_map.get(campaign_id)
                if not adset_id or not campaign_pk:
                    skipped += 1
                    continue
                AdSet.objects.update_or_create(
                    id_meta_adset=adset_id,
                    defaults={
                        'id_meta_campaign_id': campaign_pk,
                        'name': (item.get('name') or '').strip()[:255],
                        'status': (item.get('status') or '').strip()[:100],
                        'created_time': self._parse_meta_datetime(item.get('created_time')),
                        'effective_status': (item.get('effective_status') or '').strip()[:100],
                    },
                )
                total += 1
        return {
            'adsets_upserted': total,
            'adsets_skipped_missing_campaign': skipped,
            'adsets_batch_errors': errors,
        }

    def sync_ads(self) -> Dict:
        assert self.client and self.dashboard_user
        total = 0
        skipped = 0
        errors = 0
        accounts = AdAccount.objects.filter(id_dashboard_user=self.dashboard_user).only('id_meta_ad_account')
        adset_map = {
            a.id_meta_adset: a.id
            for a in AdSet.objects.filter(
                id_meta_campaign__id_meta_ad_account__id_dashboard_user=self.dashboard_user
            ).only('id', 'id_meta_adset')
        }

        batch_requests = [
            {
                'relative_url': self._to_batch_relative_url(
                    self._ad_account_edge_path(account.id_meta_ad_account, 'ads'),
                    params={
                        'fields': 'id,adset_id,name,status,created_time,effective_status',
                        'limit': 200,
                    },
                ),
                'account_meta_id': account.id_meta_ad_account,
            }
            for account in accounts
        ]
        if not batch_requests:
            return {
                'ads_upserted': 0,
                'ads_skipped_missing_adset': 0,
                'ads_batch_errors': 0,
            }

        self._log('ads', f'Extraindo ads em batch para {len(batch_requests)} contas (chunk=50).')
        for request_meta, result in self._iter_batch_paginated_requests(
            batch_requests,
            entity='ads_batch',
            batch_size=50,
        ):
            if result['status_code'] >= 400:
                errors += 1
                self._log(
                    'ads',
                    f'Falha no batch de ads para conta {request_meta["account_meta_id"]}: status={result["status_code"]}.',
                )
                continue
            body = result.get('body')
            if not isinstance(body, dict):
                continue
            for item in body.get('data') or []:
                ad_id = str(item.get('id') or '').strip()
                adset_id = str(item.get('adset_id') or '').strip()
                adset_pk = adset_map.get(adset_id)
                if not ad_id or not adset_pk:
                    skipped += 1
                    continue
                Ad.objects.update_or_create(
                    id_meta_ad=ad_id,
                    defaults={
                        'id_meta_adset_id': adset_pk,
                        'name': (item.get('name') or '').strip()[:255],
                        'status': (item.get('status') or '').strip()[:100],
                        'created_time': self._parse_meta_datetime(item.get('created_time')),
                        'effective_status': (item.get('effective_status') or '').strip()[:100],
                    },
                )
                total += 1
        return {
            'ads_upserted': total,
            'ads_skipped_missing_adset': skipped,
            'ads_batch_errors': errors,
        }

    def sync_ad_insights(self, since: date, until: date) -> Dict:
        assert self.client and self.dashboard_user
        accounts = list(AdAccount.objects.filter(id_dashboard_user=self.dashboard_user).only('id_meta_ad_account'))
        ads_qs = Ad.objects.filter(
            id_meta_adset__id_meta_campaign__id_meta_ad_account__id_dashboard_user=self.dashboard_user
        ).values(
            'id',
            'id_meta_ad',
            'id_meta_adset_id',
            'id_meta_adset__id_meta_campaign_id',
        )
        ad_cache = {
            row['id_meta_ad']: (
                row['id'],
                row['id_meta_adset_id'],
                row['id_meta_adset__id_meta_campaign_id'],
            )
            for row in ads_qs
        }
        if not ad_cache:
            self._log('ad_insights', 'Nenhum ad encontrado para processar insights.')
            return {
                'ad_insight_rows_seen': 0,
                'ad_insight_upserts': 0,
                'adset_insight_upserts': 0,
                'campaign_insight_upserts': 0,
                'ad_insight_errors': 0,
            }

        adset_agg = defaultdict(self._empty_agg)
        campaign_agg = defaultdict(self._empty_agg)

        ad_upserts = 0
        rows_seen = 0
        insight_errors = 0
        # 3-month windows balance payload size and sync throughput.
        for chunk_since, chunk_until in self._iter_month_chunks(since, until, chunk_months=3):
            chunk_since_str = chunk_since.isoformat()
            chunk_until_str = chunk_until.isoformat()
            self._log(
                'ad_insights',
                f'Processando janela trimestral de insights: {chunk_since_str} ate {chunk_until_str}',
            )
            insight_fields = 'ad_id,results,impressions,reach,spend,clicks,ctr,cpm,cpc,frequency,date_start,date_stop'
            time_range = json.dumps(
                {
                    'since': chunk_since_str,
                    'until': chunk_until_str,
                },
                separators=(',', ':'),
            )

            for account in accounts:
                account_id = str(account.id_meta_ad_account or '').strip()
                if not account_id:
                    continue
                try:
                    for insight_row in self.client.paginate(
                        self._ad_account_edge_path(account_id, 'insights'),
                        params={
                            'level': 'ad',
                            'time_range': time_range,
                            'time_increment': 1,
                            'fields': insight_fields,
                            'limit': 500,
                        },
                        entity='ad_insights',
                    ):
                        rows_seen += 1
                        ad_meta_id = str(insight_row.get('ad_id') or insight_row.get('id') or '').strip()
                        if not ad_meta_id:
                            continue
                        ad_ref = ad_cache.get(ad_meta_id)
                        if not ad_ref:
                            continue
                        ad_pk, adset_pk, campaign_pk = ad_ref
                        created_at = self._parse_insight_date(insight_row)
                        if created_at is None:
                            continue
                        metric = self._normalize_metrics(insight_row)

                        AdInsightDaily.objects.update_or_create(
                            id_meta_ad_id=ad_pk,
                            created_at=created_at,
                            defaults=self._metric_to_model_defaults(metric),
                        )
                        ad_upserts += 1

                        if adset_pk:
                            adset_key = (adset_pk, created_at)
                            adset_agg[adset_key] = self._sum_agg(adset_agg[adset_key], metric)

                        if campaign_pk:
                            campaign_key = (campaign_pk, created_at)
                            campaign_agg[campaign_key] = self._sum_agg(campaign_agg[campaign_key], metric)
                except MetaClientError as exc:
                    insight_errors += 1
                    self._log(
                        'ad_insights',
                        (
                            f'Falha no insight level=ad da conta {account_id} '
                            f'({chunk_since_str}..{chunk_until_str}): {exc}'
                        ),
                    )
                    continue

        adset_upserts = 0
        for (adset_pk, created_at), metric in adset_agg.items():
            AdSetInsightDaily.objects.update_or_create(
                id_meta_adset_id=adset_pk,
                created_at=created_at,
                defaults=self._metric_to_model_defaults(self._finalize_agg(metric)),
            )
            adset_upserts += 1

        campaign_upserts = 0
        for (campaign_pk, created_at), metric in campaign_agg.items():
            CampaignInsightDaily.objects.update_or_create(
                id_meta_campaign_id=campaign_pk,
                created_at=created_at,
                defaults=self._metric_to_model_defaults(self._finalize_agg(metric)),
            )
            campaign_upserts += 1

        return {
            'ad_insight_rows_seen': rows_seen,
            'ad_insight_upserts': ad_upserts,
            'adset_insight_upserts': adset_upserts,
            'campaign_insight_upserts': campaign_upserts,
            'ad_insight_errors': insight_errors,
        }

    def sync_facebook_pages(self) -> Dict[str, Dict]:
        assert self.client and self.dashboard_user
        page_map: Dict[str, Dict] = {}
        total = 0
        for item in self.client.paginate(
            'me/accounts',
            params={'fields': 'id,name,instagram_business_account{id,username}', 'limit': 100},
            entity='facebook_pages',
        ):
            page_id = str(item.get('id') or '').strip()
            if not page_id:
                continue
            page_name = (item.get('name') or '').strip()[:255]
            FacebookPage.objects.update_or_create(
                id_meta_page=page_id,
                defaults={
                    'name': page_name,
                    'dashboard_user_id': self.dashboard_user,
                },
            )
            page_map[page_id] = item
            total += 1
        self._log('facebook_pages', f'Paginas sincronizadas: {total}')
        return page_map

    def sync_instagram_accounts_and_insights(
        self,
        page_map: Dict[str, Dict],
        since: date,
        until: date,
    ) -> Dict:
        assert self.client and self.dashboard_user
        pages = FacebookPage.objects.filter(dashboard_user_id=self.dashboard_user).only('id', 'id_meta_page', 'name')

        upserted = 0
        with_insights = 0

        for page in pages:
            snapshot = page_map.get(page.id_meta_page) or {}
            ig_info = snapshot.get('instagram_business_account')
            if not ig_info:
                detail = self.client.request_with_retry(
                    'GET',
                    f'{page.id_meta_page}',
                    params={'fields': 'instagram_business_account{id,username}'},
                    entity='instagram_accounts',
                )
                ig_info = (detail or {}).get('instagram_business_account')

            ig_id = str((ig_info or {}).get('id') or '').strip()
            if not ig_id:
                continue

            ig_name = str((ig_info or {}).get('username') or page.name or '').strip()[:255]
            instagram_account, _ = InstagramAccount.objects.update_or_create(
                id_meta_instagram=ig_id,
                defaults={
                    'id_page': page,
                    'name': ig_name,
                },
            )
            upserted += 1

            insights_payload = self._fetch_instagram_account_insights(ig_id, since, until)
            parsed = self._parse_instagram_account_insights(insights_payload)
            if parsed:
                InstagramAccount.objects.filter(id=instagram_account.id).update(**parsed)
                with_insights += 1

        return {
            'instagram_accounts_upserted': upserted,
            'instagram_accounts_with_insights': with_insights,
        }

    def sync_media_and_insights(self, since: date, until: date) -> Dict:
        assert self.client and self.dashboard_user
        accounts = InstagramAccount.objects.filter(id_page__dashboard_user_id=self.dashboard_user).only(
            'id', 'id_meta_instagram'
        )

        media_upserts = 0
        media_insight_updates = 0
        media_insight_errors = 0

        for ig_account in accounts:
            media_batch_calls = []
            media_batch_meta: List[Tuple[str, List[str]]] = []
            for media in self.client.paginate(
                f'{ig_account.id_meta_instagram}/media',
                params={
                    'fields': (
                        'id,caption,media_type,media_url,permalink,timestamp,'
                        'like_count,comments_count'
                    ),
                    'limit': 50,
                },
                entity='instagram_media',
            ):
                media_id = str(media.get('id') or '').strip()
                if not media_id:
                    continue
                media_timestamp = self._parse_meta_datetime(media.get('timestamp'))
                if media_timestamp and media_timestamp.date() < since:
                    continue

                MediaInstagram.objects.update_or_create(
                    id_meta_media=media_id,
                    defaults={
                        'id_meta_instagram': ig_account,
                        'caption': str(media.get('caption') or ''),
                        'media_type': str(media.get('media_type') or '')[:50],
                        'media_url': str(media.get('media_url') or '')[:1000],
                        'permalink': str(media.get('permalink') or '')[:500],
                        'timestamp': media_timestamp,
                        'likes': self._to_int(media.get('like_count')),
                        'comments': self._to_int(media.get('comments_count')),
                    },
                )
                media_upserts += 1
                metrics = self._media_metrics_for_type(str(media.get('media_type') or ''))
                if metrics:
                    media_batch_calls.append(
                        {
                            'method': 'GET',
                            'relative_url': f'{media_id}/insights?metric={",".join(metrics)}',
                        }
                    )
                    media_batch_meta.append((media_id, metrics))

            if not media_batch_calls:
                continue

            batch_results = self.client.batch_request(
                media_batch_calls,
                entity=f'instagram_media_insights_{ig_account.id_meta_instagram}',
                batch_size=50,
            )

            for idx, result in enumerate(batch_results):
                media_id, metrics = media_batch_meta[idx]
                if result['status_code'] >= 400:
                    media_insight_errors += 1
                    error_detail = self._extract_batch_error_message(result)
                    detail_suffix = f'; erro={error_detail}' if error_detail else ''
                    self._log(
                        'instagram_media_insights',
                        (
                            f'Falha no insight da midia {media_id}: status={result["status_code"]}; '
                            f'metrics={",".join(metrics)}{detail_suffix}'
                        ),
                    )
                    continue

                body = result.get('body')
                if not isinstance(body, dict):
                    continue
                metric_updates = self._parse_media_insights(body.get('data') or [])
                if not metric_updates:
                    continue
                MediaInstagram.objects.filter(id_meta_media=media_id).update(**metric_updates)
                media_insight_updates += 1

        return {
            'media_upserts': media_upserts,
            'media_insight_updates': media_insight_updates,
            'media_insight_errors': media_insight_errors,
        }

    def _run_stage(self, name: str, fn, *args):
        started = time.monotonic()
        self._log('stage', f'[{name}] inicio')
        result = fn(*args)
        elapsed = time.monotonic() - started
        self._log('stage', f'[{name}] concluido em {elapsed:.2f}s. Resultado={result}')
        return result

    def _set_status(self, status_value: str) -> None:
        assert self.sync_run is not None
        self.sync_run.status = status_value
        self.sync_run.save(update_fields=['status'])

    def _finish(self, status_value: str) -> None:
        assert self.sync_run is not None
        self.sync_run.status = status_value
        self.sync_run.finished_at = timezone.now()
        self.sync_run.save(update_fields=['status', 'finished_at'])

    def _log(self, entidade: str, mensagem: str) -> None:
        logger.info('[sync:%s] %s', entidade, mensagem)
        if self.sync_run is None:
            return
        SyncLog.objects.create(
            sync_run=self.sync_run,
            entidade=entidade[:100],
            mensagem=mensagem,
        )

    def _build_date_window(self) -> Tuple[date, date]:
        today = timezone.localdate()
        if self.insights_days_override is not None:
            return today - timedelta(days=self.insights_days_override), today
        return self._subtract_months(today, 24), today

    def _normalize_insights_days_override(self, value: Optional[int]) -> Optional[int]:
        if value in (None, ''):
            return None
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        if parsed < 1:
            return None
        return parsed

    def _ad_account_edge_path(self, ad_account_id: str, edge: str) -> str:
        account = str(ad_account_id or '').strip()
        if not account:
            raise ValueError('ad_account_id is required to build Meta ad account edge path')
        normalized = account if account.startswith('act_') else f'act_{account}'
        return f'{normalized}/{edge}'

    def _to_batch_relative_url(self, path_or_url: str, params: Optional[Dict] = None) -> str:
        candidate = str(path_or_url or '').strip()
        if not candidate:
            return ''

        if candidate.startswith('http://') or candidate.startswith('https://'):
            parsed = urlparse(candidate)
            relative_path = parsed.path.lstrip('/')
            graph_version = str(getattr(self.client, 'graph_version', '') or '').strip('/')
            version_prefix = f'{graph_version}/' if graph_version else ''
            if version_prefix and relative_path.startswith(version_prefix):
                relative_path = relative_path[len(version_prefix) :]
            query_pairs = [
                (key, value)
                for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if key != 'access_token'
            ]
            query = urlencode(query_pairs, doseq=True)
            return f'{relative_path}?{query}' if query else relative_path

        relative_path = candidate.lstrip('/')
        if not params:
            return relative_path
        query = urlencode(params, doseq=True)
        return f'{relative_path}?{query}' if query else relative_path

    def _next_page_relative_url(self, current_relative_url: str, body: Dict) -> Optional[str]:
        paging = body.get('paging') if isinstance(body, dict) else None
        if not isinstance(paging, dict):
            return None

        next_url = paging.get('next')
        if next_url:
            return self._to_batch_relative_url(str(next_url))

        cursors = paging.get('cursors') or {}
        after_cursor = (cursors or {}).get('after')
        if not after_cursor:
            return None

        parsed_current = urlparse(f'/{str(current_relative_url or "").lstrip("/")}')
        base_path = parsed_current.path.lstrip('/')
        query_pairs = [
            (key, value)
            for key, value in parse_qsl(parsed_current.query, keep_blank_values=True)
            if key != 'after'
        ]
        query_pairs.append(('after', str(after_cursor)))
        query = urlencode(query_pairs, doseq=True)
        return f'{base_path}?{query}' if query else base_path

    def _iter_batch_paginated_requests(self, requests_meta: List[Dict], *, entity: str, batch_size: int = 50):
        assert self.client
        pending: List[Dict] = list(requests_meta)
        while pending:
            current_chunk = pending[:batch_size]
            pending = pending[batch_size:]
            calls = [{'method': 'GET', 'relative_url': item['relative_url']} for item in current_chunk]
            results = self.client.batch_request(
                calls,
                entity=entity,
                batch_size=batch_size,
            )
            for request_meta, result in zip(current_chunk, results):
                if result['status_code'] < 400:
                    body = result.get('body')
                    if isinstance(body, dict):
                        next_relative_url = self._next_page_relative_url(request_meta['relative_url'], body)
                        if next_relative_url:
                            next_request = dict(request_meta)
                            next_request['relative_url'] = next_relative_url
                            pending.append(next_request)
                yield request_meta, result

    def _subtract_months(self, base_date: date, months: int) -> date:
        month_idx = (base_date.year * 12 + (base_date.month - 1)) - months
        year = month_idx // 12
        month = month_idx % 12 + 1
        day = min(base_date.day, calendar.monthrange(year, month)[1])
        return date(year, month, day)

    def _iter_month_chunks(self, since: date, until: date, chunk_months: int = 3):
        if chunk_months < 1:
            raise ValueError('chunk_months must be >= 1')
        current = since
        while current <= until:
            next_start = self._add_months(current, chunk_months)
            chunk_end = min(next_start - timedelta(days=1), until)
            yield current, chunk_end
            current = chunk_end + timedelta(days=1)

    def _iter_day_chunks(self, since: date, until: date, max_span_days: int = 29):
        if max_span_days < 0:
            raise ValueError('max_span_days must be >= 0')
        current = since
        while current <= until:
            chunk_end = min(current + timedelta(days=max_span_days), until)
            yield current, chunk_end
            current = chunk_end + timedelta(days=1)

    def _add_months(self, base_date: date, months: int) -> date:
        month_idx = (base_date.year * 12 + (base_date.month - 1)) + months
        year = month_idx // 12
        month = month_idx % 12 + 1
        day = min(base_date.day, calendar.monthrange(year, month)[1])
        return date(year, month, day)

    def _parse_meta_datetime(self, value) -> Optional[datetime]:
        if not value:
            return None
        dt = parse_datetime(str(value))
        if dt is None:
            return None
        if timezone.is_naive(dt):
            return dt.replace(tzinfo=dt_timezone.utc)
        return dt

    def _parse_insight_date(self, row: Dict) -> Optional[date]:
        raw = row.get('date_start') or row.get('date_stop')
        if not raw:
            return None
        return parse_date(str(raw))

    def _to_int(self, value) -> int:
        if value in (None, ''):
            return 0
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, Decimal):
            return int(value)
        try:
            return int(float(str(value).replace(',', '')))
        except (TypeError, ValueError):
            return 0

    def _to_decimal(self, value) -> Decimal:
        if value in (None, ''):
            return Decimal('0')
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return Decimal('0')

    def _extract_results_list_value(self, values) -> int:
        if not isinstance(values, list):
            return 0
        total_values = []
        default_window_values = []
        for item in values:
            if not isinstance(item, dict):
                continue
            parsed = self._extract_metric_value(item.get('value'))
            if parsed is None:
                continue
            total_values.append(parsed)
            windows = item.get('attribution_windows')
            if isinstance(windows, list) and any(str(window).strip().lower() == 'default' for window in windows):
                default_window_values.append(parsed)
        if default_window_values:
            return sum(default_window_values)
        return sum(total_values)

    def _extract_results_value(self, value) -> int:
        # Meta may return results as scalar, as dict, or as a list containing
        # rows like {indicator, value} / {indicator, values:[{value,...}]}.
        if isinstance(value, list):
            total = 0
            for entry in value:
                if not isinstance(entry, dict):
                    continue
                if isinstance(entry.get('values'), list):
                    total += self._extract_results_list_value(entry.get('values'))
                    continue
                total += self._to_int(entry.get('value'))
            return total

        if isinstance(value, dict):
            if isinstance(value.get('values'), list):
                return self._extract_results_list_value(value.get('values'))
            return self._to_int(value.get('value'))

        return self._to_int(value)

    def _normalize_metrics(self, row: Dict) -> Dict:
        spend = self._to_decimal(row.get('spend'))
        impressions = self._to_int(row.get('impressions'))
        reach = self._to_int(row.get('reach'))
        clicks = self._to_int(row.get('clicks'))
        results = self._extract_results_value(row.get('results'))

        ctr = self._to_decimal(row.get('ctr'))
        cpm = self._to_decimal(row.get('cpm'))
        cpc = self._to_decimal(row.get('cpc'))
        frequency = self._to_decimal(row.get('frequency'))

        if impressions > 0 and ctr == 0:
            ctr = (Decimal(clicks) / Decimal(impressions)) * Decimal('100')
        if impressions > 0 and cpm == 0:
            cpm = (spend / Decimal(impressions)) * Decimal('1000')
        if clicks > 0 and cpc == 0:
            cpc = spend / Decimal(clicks)
        if reach > 0 and frequency == 0:
            frequency = Decimal(impressions) / Decimal(reach)

        return {
            'spend': spend,
            'impressions': impressions,
            'reach': reach,
            'clicks': clicks,
            'results': results,
            'ctr': ctr,
            'cpm': cpm,
            'cpc': cpc,
            'frequency': frequency,
        }

    def _metric_to_model_defaults(self, metric: Dict) -> Dict:
        return {
            'gasto_diario': metric['spend'],
            'impressao_diaria': metric['impressions'],
            'alcance_diario': metric['reach'],
            'quantidade_clicks_diaria': metric['clicks'],
            'quantidade_results_diaria': metric['results'],
            'ctr_medio': metric['ctr'],
            'cpm_medio': metric['cpm'],
            'cpc_medio': metric['cpc'],
            'frequencia_media': metric['frequency'],
        }

    def _empty_agg(self) -> Dict:
        return {
            'spend': Decimal('0'),
            'impressions': 0,
            'reach': 0,
            'clicks': 0,
            'results': 0,
            'ctr': Decimal('0'),
            'cpm': Decimal('0'),
            'cpc': Decimal('0'),
            'frequency': Decimal('0'),
        }

    def _sum_agg(self, left: Dict, right: Dict) -> Dict:
        return {
            'spend': left['spend'] + right['spend'],
            'impressions': left['impressions'] + right['impressions'],
            'reach': left['reach'] + right['reach'],
            'clicks': left['clicks'] + right['clicks'],
            'results': left['results'] + right['results'],
            'ctr': Decimal('0'),
            'cpm': Decimal('0'),
            'cpc': Decimal('0'),
            'frequency': Decimal('0'),
        }

    def _finalize_agg(self, agg: Dict) -> Dict:
        spend = agg['spend']
        impressions = agg['impressions']
        reach = agg['reach']
        clicks = agg['clicks']
        results = agg['results']

        ctr = Decimal('0')
        cpm = Decimal('0')
        cpc = Decimal('0')
        frequency = Decimal('0')

        if impressions > 0:
            ctr = (Decimal(clicks) / Decimal(impressions)) * Decimal('100')
            cpm = (spend / Decimal(impressions)) * Decimal('1000')
        if clicks > 0:
            cpc = spend / Decimal(clicks)
        if reach > 0:
            frequency = Decimal(impressions) / Decimal(reach)

        return {
            'spend': spend,
            'impressions': impressions,
            'reach': reach,
            'clicks': clicks,
            'results': results,
            'ctr': ctr,
            'cpm': cpm,
            'cpc': cpc,
            'frequency': frequency,
        }

    def _fetch_instagram_account_insights(self, ig_id: str, since: date, until: date) -> Dict:
        assert self.client
        effective_since = since
        effective_until = until
        # Meta may reject boundary dates exactly at "2 years"; keep a 2-day safety margin.
        min_allowed_since = self._subtract_months(timezone.localdate(), 24) + timedelta(days=2)

        if effective_since < min_allowed_since:
            self._log(
                'instagram_account_insights',
                (
                    f'Ajustando janela da conta {ig_id} para limite de 2 anos (margem +2d): '
                    f'since {effective_since.isoformat()} -> {min_allowed_since.isoformat()}'
                ),
            )
            effective_since = min_allowed_since

        if effective_since > effective_until:
            self._log(
                'instagram_account_insights',
                (
                    f'Janela ignorada para conta {ig_id}: since={effective_since.isoformat()} '
                    f'e maior que until={effective_until.isoformat()} apos ajuste de 2 anos.'
                ),
            )
            return {'data': []}

        metrics_regular = ['reach']
        metrics_total_value = ['views', 'content_views', 'profile_views', 'accounts_engaged', 'follows_and_unfollows']
        metrics = metrics_regular + metrics_total_value + ['follower_count']
        metric_entries: Dict[str, Dict] = {}
        date_windows = list(self._iter_day_chunks(effective_since, effective_until, max_span_days=29))

        def merge_metric_entries(payload: Dict) -> None:
            if not isinstance(payload, dict):
                return
            for entry in (payload.get('data') or []):
                if not isinstance(entry, dict):
                    continue
                metric_name = str(entry.get('name') or '').strip()
                if not metric_name:
                    continue
                values = entry.get('values') or []
                if metric_name not in metric_entries:
                    metric_entries[metric_name] = {
                        'name': metric_name,
                        'values': [],
                    }
                if isinstance(values, list):
                    metric_entries[metric_name]['values'].extend(v for v in values if isinstance(v, dict))

        for window_since, window_until in date_windows:
            params_window = {
                'period': 'day',
                'since': window_since.isoformat(),
                'until': window_until.isoformat(),
            }
            for metric_group, extra_params in (
                (metrics_regular, {}),
                (metrics_total_value, {'metric_type': 'total_value'}),
            ):
                if not metric_group:
                    continue
                try:
                    payload = self.client.request_with_retry(
                        'GET',
                        f'{ig_id}/insights',
                        params={
                            'metric': ','.join(metric_group),
                            **params_window,
                            **extra_params,
                        },
                        entity='instagram_account_insights',
                    )
                    merge_metric_entries(payload)
                except MetaClientError as exc:
                    self._log(
                        'instagram_account_insights',
                        (
                            f'Falha na chamada consolidada da conta {ig_id} '
                            f'({window_since.isoformat()}..{window_until.isoformat()}). '
                            f'Tentando fallback por metrica: {exc}'
                        ),
                    )

        missing_metrics = [metric for metric in metrics if metric not in metric_entries]
        metrics_total_value_set = set(metrics_total_value)
        for metric in missing_metrics:
            if metric == 'follower_count':
                continue
            metric_extra_params = {'metric_type': 'total_value'} if metric in metrics_total_value_set else {}
            metric_error = None
            for window_since, window_until in date_windows:
                try:
                    payload = self.client.request_with_retry(
                        'GET',
                        f'{ig_id}/insights',
                        params={
                            'metric': metric,
                            'period': 'day',
                            'since': window_since.isoformat(),
                            'until': window_until.isoformat(),
                            **metric_extra_params,
                        },
                        entity='instagram_account_insights',
                    )
                    merge_metric_entries(payload)
                except MetaClientError as exc:
                    metric_error = exc
            if metric not in metric_entries and metric_error is not None:
                self._log(
                    'instagram_account_insights',
                    f'Metrica indisponivel para conta {ig_id}: {metric}. Motivo: {metric_error}',
                )

        # follower_count is stricter than other metrics. Keep an extra 1-day safety margin
        # to avoid timezone boundary issues on Meta's side.
        follower_today_guard = timezone.localdate() - timedelta(days=2)
        follower_until = min(effective_until, follower_today_guard)
        follower_since = max(effective_since, follower_until - timedelta(days=27))
        if follower_since <= follower_until:
            try:
                payload = self.client.request_with_retry(
                    'GET',
                    f'{ig_id}/insights',
                    params={
                        'metric': 'follower_count',
                        'period': 'day',
                        'since': follower_since.isoformat(),
                        'until': follower_until.isoformat(),
                    },
                    entity='instagram_account_insights',
                )
                merge_metric_entries(payload)
            except MetaClientError as exc:
                exc_message = str(exc).lower()
                supports_last_30_days_error = (
                    'follower_count' in exc_message
                    and 'last 30 days excluding the current day' in exc_message
                )
                if supports_last_30_days_error:
                    retry_until = timezone.localdate() - timedelta(days=2)
                    retry_since = max(effective_since, retry_until - timedelta(days=7))
                    if retry_since <= retry_until:
                        try:
                            payload = self.client.request_with_retry(
                                'GET',
                                f'{ig_id}/insights',
                                params={
                                    'metric': 'follower_count',
                                    'period': 'day',
                                    'since': retry_since.isoformat(),
                                    'until': retry_until.isoformat(),
                                },
                                entity='instagram_account_insights',
                            )
                            merge_metric_entries(payload)
                            self._log(
                                'instagram_account_insights',
                                (
                                    f'Retry follower_count aplicado para conta {ig_id}: '
                                    f'{retry_since.isoformat()}..{retry_until.isoformat()}.'
                                ),
                            )
                            return {'data': list(metric_entries.values())}
                        except MetaClientError as retry_exc:
                            self._log(
                                'instagram_account_insights',
                                (
                                    f'Metrica indisponivel para conta {ig_id}: follower_count. '
                                    f'Motivo: {retry_exc}'
                                ),
                            )
                    else:
                        self._log(
                            'instagram_account_insights',
                            (
                                f'Retry follower_count ignorado para conta {ig_id}: '
                                f'since={retry_since.isoformat()} until={retry_until.isoformat()}.'
                            ),
                        )
                else:
                    self._log(
                        'instagram_account_insights',
                        (
                            f'Metrica indisponivel para conta {ig_id}: follower_count. '
                            f'Motivo: {exc}'
                        ),
                    )
        else:
            self._log(
                'instagram_account_insights',
                (
                    f'Janela sem dados validos para follower_count da conta {ig_id}: '
                    f'since={follower_since.isoformat()} until={follower_until.isoformat()}.'
                ),
            )

        return {'data': list(metric_entries.values())}

    def _media_metrics_for_type(self, media_type: str) -> List[str]:
        kind = (media_type or '').upper()
        common = ['reach', 'saved', 'shares']
        if kind in {'REEL', 'REELS'}:
            return common + ['views', 'plays', 'ig_reels_video_view_total_time', 'ig_reels_avg_watch_time']
        if kind in {'VIDEO'}:
            return common + ['views']
        if kind in {'IMAGE', 'CAROUSEL_ALBUM'}:
            return common
        # fallback para tipos desconhecidos
        return common + ['views']

    def _parse_instagram_account_insights(self, payload: Dict) -> Dict:
        if not isinstance(payload, dict):
            return {}

        metric_map = {}
        for entry in payload.get('data') or []:
            metric_name = entry.get('name')
            values = entry.get('values') or []
            parsed_values = [self._extract_metric_value(v.get('value')) for v in values if isinstance(v, dict)]
            parsed_values = [v for v in parsed_values if v is not None]
            if not metric_name or not parsed_values:
                continue
            if metric_name == 'follower_count':
                metric_map[metric_name] = parsed_values[-1]
            else:
                metric_map[metric_name] = sum(parsed_values)

        updates = {}
        reach_value = metric_map.get('reach', metric_map.get('accounts_reached'))
        if reach_value is not None:
            updates['accounts_reached'] = self._to_int(reach_value)

        impressions_value = metric_map.get('views', metric_map.get('content_views', metric_map.get('impressions')))
        if impressions_value is not None:
            updates['impressions'] = self._to_int(impressions_value)
        if 'profile_views' in metric_map:
            updates['profile_views'] = self._to_int(metric_map['profile_views'])
        if 'accounts_engaged' in metric_map:
            updates['accounts_engaged'] = self._to_int(metric_map['accounts_engaged'])
        if 'follower_count' in metric_map:
            updates['follower_count'] = self._to_int(metric_map['follower_count'])
        if 'follows_and_unfollows' in metric_map:
            updates['follows_and_unfollows'] = self._to_int(metric_map['follows_and_unfollows'])
        return updates

    def _extract_metric_value(self, value):
        if isinstance(value, (int, float, Decimal)):
            return int(value)
        if isinstance(value, dict):
            if 'value' in value:
                return self._to_int(value.get('value'))
            if 'total_value' in value:
                return self._to_int(value.get('total_value'))
            if 'count' in value:
                return self._to_int(value.get('count'))
            return None
        try:
            return int(float(str(value)))
        except (TypeError, ValueError):
            return None

    def _extract_batch_error_message(self, result: Dict) -> str:
        body = result.get('body')
        if isinstance(body, dict):
            error = body.get('error')
            if isinstance(error, dict):
                message = str(error.get('message') or '').strip()
                if message:
                    return message
        body_raw = str(result.get('body_raw') or '').strip()
        return body_raw[:400] if body_raw else ''

    def _parse_media_insights(self, insights_data) -> Dict:
        metric_values = {}
        for entry in insights_data:
            if not isinstance(entry, dict):
                continue
            name = entry.get('name')
            values = entry.get('values') or []
            if name == 'ig_reels_avg_watch_time':
                parsed_values = [self._to_decimal(v.get('value')) for v in values if isinstance(v, dict)]
                parsed_values = [v for v in parsed_values if v is not None]
                if not name or not parsed_values:
                    continue
                metric_values[name] = max(parsed_values)
                continue
            parsed_values = [self._extract_metric_value(v.get('value')) for v in values if isinstance(v, dict)]
            parsed_values = [v for v in parsed_values if v is not None]
            if not name or not parsed_values:
                continue
            metric_values[name] = max(parsed_values)

        updates = {}
        if 'reach' in metric_values:
            updates['reach'] = self._to_int(metric_values['reach'])
        views_value = metric_values.get('views', metric_values.get('video_views', metric_values.get('content_views')))
        if views_value is not None:
            updates['views'] = self._to_int(views_value)
        if 'saved' in metric_values:
            updates['saved'] = self._to_int(metric_values['saved'])
        if 'shares' in metric_values:
            updates['shares'] = self._to_int(metric_values['shares'])
        if 'plays' in metric_values:
            updates['plays'] = self._to_int(metric_values['plays'])
        watch_time_value = metric_values.get('ig_reels_video_view_total_time', metric_values.get('total_watch_time'))
        if watch_time_value is not None:
            updates['watch_time'] = self._to_int(watch_time_value)
        if 'ig_reels_avg_watch_time' in metric_values:
            updates['avg_watch_time'] = self._to_decimal(metric_values['ig_reels_avg_watch_time'])
        return updates
