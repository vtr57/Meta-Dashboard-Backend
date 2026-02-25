from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Optional

import requests
from django.conf import settings
from django.utils import timezone


PREVENTIVE_RENEWAL_DAYS = 50


class MetaTokenExchangeError(Exception):
    def __init__(self, detail: str, status_code: int) -> None:
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code


def _meta_error_message(payload, fallback: str) -> str:
    if isinstance(payload, dict):
        error = payload.get('error')
        if isinstance(error, dict) and error.get('message'):
            return str(error['message'])
    return fallback


def _parse_positive_int(value) -> Optional[int]:
    if value in (None, ''):
        return None
    if isinstance(value, bool):
        return None
    try:
        parsed = int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _meta_expired_at_from_payload(payload) -> Optional[datetime]:
    if not isinstance(payload, dict):
        return None

    expires_in = _parse_positive_int(payload.get('expires_in'))
    if expires_in is not None:
        return timezone.now() + timedelta(seconds=expires_in)

    expires_at = _parse_positive_int(payload.get('expires_at'))
    if expires_at is not None:
        return datetime.fromtimestamp(expires_at, tz=dt_timezone.utc)

    return None


def _meta_fetch_expired_at_with_debug_token(
    *,
    graph_version: str,
    app_id: str,
    app_secret: str,
    input_token: str,
) -> Optional[datetime]:
    if not app_id or not app_secret or not input_token:
        return None

    url = f'https://graph.facebook.com/{graph_version}/debug_token'
    params = {
        'input_token': input_token,
        'access_token': f'{app_id}|{app_secret}',
    }
    try:
        response = requests.get(url, params=params, timeout=30)
    except requests.RequestException:
        return None

    try:
        payload = response.json()
    except ValueError:
        payload = {}

    if response.status_code >= 400:
        return None

    data = payload.get('data') if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return None

    expires_at = _parse_positive_int(data.get('expires_at'))
    if expires_at is None:
        return None
    return datetime.fromtimestamp(expires_at, tz=dt_timezone.utc)


def _meta_preventive_expired_at() -> datetime:
    # When Meta does not return expiration metadata, force a preventive renewal window.
    return timezone.now() + timedelta(days=PREVENTIVE_RENEWAL_DAYS)


def exchange_short_token_for_long_token(
    *,
    short_token: str,
    graph_version: Optional[str] = None,
    app_id: Optional[str] = None,
    app_secret: Optional[str] = None,
) -> dict:
    short_token = str(short_token or '').strip()
    if not short_token:
        raise MetaTokenExchangeError('short_token obrigatorio para troca por long token.', 400)

    graph_version = str(
        graph_version or getattr(settings, 'META_GRAPH_VERSION', 'v24.0') or 'v24.0'
    ).strip('/')
    app_id = str(app_id or getattr(settings, 'META_APP_ID', '') or '').strip()
    app_secret = str(app_secret or getattr(settings, 'META_APP_SECRET', '') or '').strip()

    if not app_id or not app_secret:
        raise MetaTokenExchangeError(
            'META_APP_ID e META_APP_SECRET precisam estar configurados no backend.',
            500,
        )

    url = f'https://graph.facebook.com/{graph_version}/oauth/access_token'
    params = {
        'grant_type': 'fb_exchange_token',
        'client_id': app_id,
        'client_secret': app_secret,
        'fb_exchange_token': short_token,
    }

    try:
        response = requests.get(url, params=params, timeout=30)
    except requests.RequestException as exc:
        raise MetaTokenExchangeError(f'Falha de rede ao trocar token: {exc}', 502) from exc

    try:
        payload = response.json()
    except ValueError:
        payload = {}

    if response.status_code >= 400:
        raise MetaTokenExchangeError(
            _meta_error_message(payload, 'Falha ao trocar short token por long token.'),
            400,
        )

    long_token = str(payload.get('access_token') or '').strip()
    if not long_token:
        raise MetaTokenExchangeError(
            'Meta Graph API nao retornou access_token na troca.',
            502,
        )

    expired_at = _meta_expired_at_from_payload(payload)
    expiration_source = 'exchange'
    if expired_at is None:
        expired_at = _meta_fetch_expired_at_with_debug_token(
            graph_version=graph_version,
            app_id=app_id,
            app_secret=app_secret,
            input_token=long_token,
        )
        expiration_source = 'debug_token'
    if expired_at is None:
        expired_at = _meta_preventive_expired_at()
        expiration_source = f'preventive_{PREVENTIVE_RENEWAL_DAYS}d'

    return {
        'long_token': long_token,
        'expired_at': expired_at,
        'expiration_source': expiration_source,
        'graph_version': graph_version,
    }
