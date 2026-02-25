import json
import logging
import math
import time
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qs, urlparse

import requests

from Dashboard.models import SyncLog, SyncRun


logger = logging.getLogger(__name__)


class MetaClientError(Exception):
    pass


class MetaGraphClient:
    def __init__(
        self,
        access_token: str,
        sync_run: Optional[SyncRun] = None,
        graph_version: str = 'v24.0',
        base_url: str = 'https://graph.facebook.com',
        request_pause_seconds: float = 0.6,
        timeout_seconds: int = 30,
        max_retries: int = 5,
        batch_size: int = 20,
    ) -> None:
        if not access_token:
            raise ValueError('access_token is required')
        if max_retries < 1:
            raise ValueError('max_retries must be >= 1')
        if batch_size < 1:
            raise ValueError('batch_size must be >= 1')

        self.access_token = access_token
        self.sync_run = sync_run
        self.graph_version = graph_version.strip('/')
        self.base_url = base_url.rstrip('/')
        self.request_pause_seconds = max(0.0, request_pause_seconds)
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.session = requests.Session()

    def request_with_retry(
        self,
        method: str,
        path_or_url: str,
        *,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        json_body: Optional[Dict] = None,
        entity: str = 'meta_graph',
        timeout_seconds: Optional[int] = None,
    ) -> Any:
        method = method.upper()
        url = self._build_url(path_or_url)
        request_params = dict(params or {})
        if 'access_token' not in request_params and 'access_token=' not in url:
            request_params['access_token'] = self.access_token
        timeout = timeout_seconds or self.timeout_seconds

        for attempt in range(1, self.max_retries + 1):
            self._log(entity, f'Request attempt {attempt}/{self.max_retries}: {method} {self._redact_url(url)}')
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=request_params,
                    data=data,
                    json=json_body,
                    timeout=timeout,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    self._log(entity, f'Request failed after retries: {exc}')
                    raise MetaClientError(f'Network error calling Meta Graph API: {exc}') from exc

                wait_seconds = self._backoff_seconds(attempt)
                self._log(entity, f'Network error: {exc}. Retrying in {wait_seconds:.1f}s.')
                time.sleep(wait_seconds)
                continue

            if 200 <= response.status_code < 300:
                payload = self._safe_json(response)
                self._log(entity, f'Request success status={response.status_code}.')
                if self.request_pause_seconds > 0:
                    time.sleep(self.request_pause_seconds)
                return payload

            payload = self._safe_json(response)
            error_message = self._extract_error_message(payload, response.text)
            retriable = self._is_retriable(response.status_code)
            if retriable and attempt < self.max_retries:
                wait_seconds = self._backoff_seconds(attempt)
                self._log(
                    entity,
                    f'Request error status={response.status_code}: {error_message}. '
                    f'Retrying in {wait_seconds:.1f}s.',
                )
                time.sleep(wait_seconds)
                continue

            self._log(entity, f'Request failed status={response.status_code}: {error_message}')
            raise MetaClientError(f'Meta Graph API error ({response.status_code}): {error_message}')

        raise MetaClientError('Unexpected retry flow termination.')

    def paginate(
        self,
        path_or_url: str,
        *,
        params: Optional[Dict] = None,
        entity: str = 'meta_graph',
        page_limit: Optional[int] = None,
    ) -> Iterable[Dict]:
        current_path_or_url = path_or_url
        current_params = dict(params or {})
        page = 1

        while current_path_or_url:
            if page_limit is not None and page > page_limit:
                self._log(entity, f'Pagination stopped by page_limit={page_limit}.')
                return

            self._log(entity, f'Fetching page {page}.')
            try:
                payload = self.request_with_retry(
                    method='GET',
                    path_or_url=current_path_or_url,
                    params=current_params,
                    entity=entity,
                )
            except MetaClientError as exc:
                self._log(entity, f'Pagination error on page {page}: {exc}')
                raise

            items = payload.get('data') if isinstance(payload, dict) else None
            if not isinstance(items, list):
                items = []

            self._log(entity, f'Page {page} received {len(items)} rows.')
            for item in items:
                yield item

            paging = payload.get('paging') if isinstance(payload, dict) else None
            next_url = paging.get('next') if isinstance(paging, dict) else None
            after_cursor = None
            if isinstance(paging, dict):
                cursors = paging.get('cursors') or {}
                after_cursor = cursors.get('after')
                if not after_cursor and next_url:
                    query_values = parse_qs(urlparse(next_url).query)
                    after_cursor = (query_values.get('after') or [None])[0]

            if next_url:
                self._log(entity, f'Next page detected via paging.next (after={after_cursor}).')
                current_path_or_url = next_url
                current_params = {}
                page += 1
                continue

            if after_cursor:
                self._log(entity, f'Next page detected via cursor after={after_cursor}.')
                current_path_or_url = path_or_url
                current_params = dict(params or {})
                current_params['after'] = after_cursor
                page += 1
                continue

            self._log(entity, f'Pagination finished at page {page}.')
            return

    def batch_request(
        self,
        calls: List[Dict],
        *,
        entity: str = 'meta_batch',
        batch_size: Optional[int] = None,
        include_headers: bool = False,
    ) -> List[Dict]:
        if not calls:
            self._log(entity, 'batch_request called with 0 calls.')
            return []

        size = batch_size or self.batch_size
        if size < 1:
            raise ValueError('batch_size must be >= 1')

        total_chunks = int(math.ceil(len(calls) / size))
        aggregated_results: List[Dict] = []

        for chunk_index, start in enumerate(range(0, len(calls), size), start=1):
            chunk = calls[start : start + size]
            self._log(
                entity,
                f'Batch chunk {chunk_index}/{total_chunks} with {len(chunk)} calls (chunk_size={size}).',
            )
            try:
                payload = self.request_with_retry(
                    method='POST',
                    path_or_url='/',
                    data={
                        'batch': json.dumps(chunk),
                        'include_headers': 'true' if include_headers else 'false',
                    },
                    entity=entity,
                )
            except MetaClientError as exc:
                self._log(entity, f'Batch chunk {chunk_index}/{total_chunks} failed: {exc}')
                raise

            if not isinstance(payload, list):
                self._log(entity, 'Unexpected batch response format (expected list).')
                raise MetaClientError('Unexpected batch response format.')

            normalized = self._normalize_batch_results(payload)
            errors = sum(1 for item in normalized if item['status_code'] >= 400)
            self._log(
                entity,
                f'Batch chunk {chunk_index} completed with {len(normalized)} results and {errors} non-2xx.',
            )
            aggregated_results.extend(normalized)

        self._log(entity, f'Batch processing finished with {len(aggregated_results)} total results.')
        return aggregated_results

    def _build_url(self, path_or_url: str) -> str:
        candidate = (path_or_url or '').strip()
        if candidate.startswith('http://') or candidate.startswith('https://'):
            return candidate

        relative = candidate.lstrip('/')
        if relative:
            return f'{self.base_url}/{self.graph_version}/{relative}'
        return f'{self.base_url}/{self.graph_version}'

    def _is_retriable(self, status_code: int) -> bool:
        return status_code in {408, 429, 500, 502, 503, 504}

    def _backoff_seconds(self, attempt: int) -> float:
        # Required exponential pattern: 2s, 4s, 8s...
        return float(2 ** attempt)

    def _extract_error_message(self, payload: Any, raw_text: str) -> str:
        if isinstance(payload, dict):
            error = payload.get('error')
            if isinstance(error, dict):
                message = error.get('message')
                if message:
                    return str(message)
        if raw_text:
            return raw_text[:400]
        return 'Unknown error'

    def _safe_json(self, response: requests.Response):
        try:
            return response.json()
        except ValueError:
            return {'raw': response.text}

    def _normalize_batch_results(self, payload: List[Dict]) -> List[Dict]:
        normalized: List[Dict] = []
        for item in payload:
            if not isinstance(item, dict):
                normalized.append(
                    {
                        'status_code': 500,
                        'headers': [],
                        'body': None,
                        'body_raw': str(item),
                    }
                )
                continue

            status_code = int(item.get('code') or 0)
            body_raw = item.get('body')
            body = body_raw
            if isinstance(body_raw, str):
                try:
                    body = json.loads(body_raw)
                except json.JSONDecodeError:
                    body = None

            normalized.append(
                {
                    'status_code': status_code,
                    'headers': item.get('headers') or [],
                    'body': body,
                    'body_raw': body_raw,
                }
            )
        return normalized

    def _redact_url(self, url: str) -> str:
        token = self.access_token
        if token and token in url:
            return url.replace(token, '***')
        return url

    def _log(self, entidade: str, mensagem: str) -> None:
        logger.info('[%s] %s', entidade, mensagem)
        if self.sync_run_id:
            try:
                SyncLog.objects.create(
                    sync_run_id=self.sync_run_id,
                    entidade=entidade[:100],
                    mensagem=mensagem,
                )
            except Exception:
                logger.exception('Failed to persist SyncLog entry.')

    @property
    def sync_run_id(self) -> Optional[int]:
        return self.sync_run.id if self.sync_run else None
