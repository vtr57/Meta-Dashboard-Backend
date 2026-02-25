import secrets
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import HttpResponseRedirect, JsonResponse
from django.urls import reverse
from django.views.decorators.http import require_GET

from Dashboard.models import DashboardUser
from loginFacebook.services import MetaTokenExchangeError, exchange_short_token_for_long_token


def _meta_error_message(payload, fallback: str) -> str:
    if isinstance(payload, dict):
        error = payload.get('error')
        if isinstance(error, dict) and error.get('message'):
            return str(error['message'])
    return fallback


def _is_absolute_http_url(value: str) -> bool:
    return value.startswith('http://') or value.startswith('https://')


def _merge_query_params(url: str, new_params: dict[str, str]) -> str:
    parsed = urlsplit(url)
    current = dict(parse_qsl(parsed.query, keep_blank_values=True))
    current.update({k: v for k, v in new_params.items() if v is not None})
    merged_query = urlencode(current)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, merged_query, parsed.fragment))


def _resolve_frontend_redirect_base(request) -> str:
    candidate = str(request.session.pop('facebook_oauth_next', '') or '').strip()
    if _is_absolute_http_url(candidate):
        return candidate

    candidate = str(getattr(settings, 'FRONTEND_CONNECTION_URL', '') or '').strip()
    if _is_absolute_http_url(candidate):
        return candidate

    candidate = str(request.headers.get('Referer') or '').strip()
    if _is_absolute_http_url(candidate):
        return candidate

    return ''


def _redirect_with_oauth_result(request, *, connected: bool, error_message: str = ''):
    base_url = _resolve_frontend_redirect_base(request)
    if not base_url:
        fallback = (
            'FRONTEND_CONNECTION_URL nao configurado para redirecionamento OAuth.'
            if connected
            else error_message or 'Falha no login com Facebook.'
        )
        return JsonResponse({'detail': fallback}, status=400)

    if connected:
        redirect_url = _merge_query_params(base_url, {'fb_connected': '1'})
    else:
        redirect_url = _merge_query_params(base_url, {'fb_error': error_message or 'oauth_failed'})
    return HttpResponseRedirect(redirect_url)


@require_GET
@login_required
def facebook_login_start(request):
    app_id = str(getattr(settings, 'META_APP_ID', '') or '').strip()
    if not app_id:
        return JsonResponse(
            {'detail': 'META_APP_ID nao configurado no backend.'},
            status=500,
        )

    graph_version = str(getattr(settings, 'META_GRAPH_VERSION', 'v24.0') or 'v24.0').strip('/')
    redirect_uri = request.build_absolute_uri(reverse('facebook-login-callback'))
    scope = str(
        getattr(
            settings,
            'FACEBOOK_LOGIN_SCOPE',
            'public_profile,email,business_management,ads_read,pages_read_engagement,instagram_basic',
        )
        or ''
    ).strip()

    params = {
        'client_id': app_id,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
    }

    oauth_state = secrets.token_urlsafe(32)
    request.session['facebook_oauth_state'] = oauth_state

    next_url = str(request.GET.get('next') or '').strip()
    if _is_absolute_http_url(next_url):
        request.session['facebook_oauth_next'] = next_url
    else:
        request.session.pop('facebook_oauth_next', None)

    params['state'] = oauth_state

    if scope:
        params['scope'] = scope

    oauth_url = f"https://www.facebook.com/{graph_version}/dialog/oauth?{urlencode(params)}"
    return HttpResponseRedirect(oauth_url)


@require_GET
@login_required
def facebook_login_callback(request):
    expected_state = str(request.session.pop('facebook_oauth_state', '') or '').strip()
    received_state = str(request.GET.get('state') or '').strip()

    if not expected_state or not received_state or not secrets.compare_digest(expected_state, received_state):
        return _redirect_with_oauth_result(
            request,
            connected=False,
            error_message='State OAuth invalido ou expirado.',
        )

    code = str(request.GET.get('code') or '').strip()
    if not code:
        error_description = str(request.GET.get('error_description') or '').strip()
        detail = error_description or 'Parametro code nao encontrado no callback OAuth.'
        return _redirect_with_oauth_result(request, connected=False, error_message=detail)

    app_id = str(getattr(settings, 'META_APP_ID', '') or '').strip()
    app_secret = str(getattr(settings, 'META_APP_SECRET', '') or '').strip()
    if not app_id or not app_secret:
        return _redirect_with_oauth_result(
            request,
            connected=False,
            error_message='META_APP_ID e META_APP_SECRET precisam estar configurados no backend.',
        )

    graph_version = str(getattr(settings, 'META_GRAPH_VERSION', 'v24.0') or 'v24.0').strip('/')
    redirect_uri = request.build_absolute_uri(reverse('facebook-login-callback'))
    token_url = f'https://graph.facebook.com/{graph_version}/oauth/access_token'
    token_params = {
        'client_id': app_id,
        'client_secret': app_secret,
        'redirect_uri': redirect_uri,
        'code': code,
    }

    try:
        token_response = requests.get(token_url, params=token_params, timeout=30)
    except requests.RequestException as exc:
        return _redirect_with_oauth_result(
            request,
            connected=False,
            error_message=f'Falha de rede ao obter short token: {exc}',
        )

    try:
        token_payload = token_response.json()
    except ValueError:
        token_payload = {}

    if token_response.status_code >= 400:
        return _redirect_with_oauth_result(
            request,
            connected=False,
            error_message=_meta_error_message(
                token_payload,
                'Falha ao trocar code por short token no Facebook.',
            ),
        )

    short_token = str(token_payload.get('access_token') or '').strip()
    if not short_token:
        return _redirect_with_oauth_result(
            request,
            connected=False,
            error_message='Facebook nao retornou access_token na troca do code.',
        )

    me_url = f'https://graph.facebook.com/{graph_version}/me'
    me_params = {
        'fields': 'id,name',
        'access_token': short_token,
    }
    try:
        me_response = requests.get(me_url, params=me_params, timeout=30)
    except requests.RequestException as exc:
        return _redirect_with_oauth_result(
            request,
            connected=False,
            error_message=f'Falha de rede ao consultar /me no Facebook: {exc}',
        )

    try:
        me_payload = me_response.json()
    except ValueError:
        me_payload = {}

    if me_response.status_code >= 400:
        return _redirect_with_oauth_result(
            request,
            connected=False,
            error_message=_meta_error_message(
                me_payload,
                'Falha ao obter dados do usuario em /me.',
            ),
        )

    id_meta_user = str(me_payload.get('id') or '').strip()
    if not id_meta_user:
        return _redirect_with_oauth_result(
            request,
            connected=False,
            error_message='Facebook /me nao retornou id do usuario.',
        )

    try:
        exchange = exchange_short_token_for_long_token(short_token=short_token, graph_version=graph_version)
    except MetaTokenExchangeError as exc:
        return _redirect_with_oauth_result(
            request,
            connected=False,
            error_message=exc.detail,
        )

    long_token = exchange['long_token']
    expired_at = exchange['expired_at']

    with transaction.atomic():
        already_linked = (
            DashboardUser.objects.select_for_update()
            .filter(id_meta_user=id_meta_user)
            .exclude(user=request.user)
            .exists()
        )
        if already_linked:
            return _redirect_with_oauth_result(
                request,
                connected=False,
                error_message='id_meta_user ja conectado a outro usuario do sistema.',
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

    return _redirect_with_oauth_result(
        request,
        connected=True,
    )
