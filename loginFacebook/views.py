import secrets
import json
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
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


def _origin_from_url(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme in {'http', 'https'} and parsed.netloc:
        return f'{parsed.scheme}://{parsed.netloc}'
    return ''


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
    popup_mode = bool(request.session.pop('facebook_oauth_popup', False))
    popup_target_origin = str(request.session.pop('facebook_oauth_target_origin', '') or '').strip()
    if popup_mode:
        if not _is_absolute_http_url(popup_target_origin):
            popup_target_origin = _origin_from_url(str(getattr(settings, 'FRONTEND_CONNECTION_URL', '') or '').strip())

        payload = {
            'type': 'facebook_oauth_result',
            'status': 'success' if connected else 'error',
            'error': (error_message or None) if not connected else None,
        }

        # Fallback content in case browser blocks window.close.
        fallback_base = _resolve_frontend_redirect_base(request)
        if fallback_base:
            if connected:
                fallback_url = _merge_query_params(fallback_base, {'fb_connected': '1'})
            else:
                fallback_url = _merge_query_params(fallback_base, {'fb_error': error_message or 'oauth_failed'})
        else:
            fallback_url = ''

        html = f"""<!doctype html>
<html lang="pt-BR">
  <head>
    <meta charset="utf-8" />
    <title>Facebook Login</title>
  </head>
  <body>
    <script>
      (function () {{
        var payload = {json.dumps(payload, ensure_ascii=False)};
        var targetOrigin = {json.dumps(popup_target_origin, ensure_ascii=False)};
        if (window.opener && !window.opener.closed) {{
          try {{
            window.opener.postMessage(payload, targetOrigin || "*");
          }} catch (err) {{}}
        }}
        window.close();
      }})();
    </script>
    <p>Finalizando login do Facebook...</p>
    {"<p><a href='" + fallback_url + "'>Voltar para a aplicacao</a></p>" if fallback_url else ""}
  </body>
</html>"""
        return HttpResponse(html)

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
        request.session['facebook_oauth_target_origin'] = _origin_from_url(next_url)
    else:
        request.session.pop('facebook_oauth_next', None)
        request.session.pop('facebook_oauth_target_origin', None)

    request.session['facebook_oauth_popup'] = str(request.GET.get('popup') or '').strip() in {'1', 'true', 'yes'}

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


@require_GET
def privacy_policy(request):
    html = """<!doctype html>
<html lang="pt-BR">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Politica de Privacidade, Meta Local Dashboard 9</title>
    <style>
      body { font-family: Arial, sans-serif; background: #f4f7fb; color: #102a4d; margin: 0; }
      .container { max-width: 860px; margin: 32px auto; background: #fff; border: 1px solid #c4d5ef; border-radius: 12px; padding: 20px; }
      h1, h2 { color: #082f6e; }
      h1 { margin-top: 0; }
      p { line-height: 1.5; }
      ul { margin-top: 8px; line-height: 1.5; }
    </style>
  </head>
  <body>
    <main class="container">
      <h1>Politica de Privacidade, Meta Local Dashboard 9</h1>
      <p>Ultima atualizacao: 25/02/2026</p>
      <p>Esta Politica de Privacidade explica como o Meta Local Dashboard 9 ("App") coleta, usa, armazena e compartilha dados quando voce cria uma conta e utiliza as funcionalidades do nosso dashboard.</p>
      <p>Ao usar o App, voce declara que leu e entendeu esta Politica.</p>

      <h2>1. Quem somos (Controlador)</h2>
      <p><strong>Controlador dos dados:</strong> Vitor Marques Tramontin Silveira</p>
      <p><strong>CNPJ/CPF:</strong> 09646268960</p>
      <p><strong>Endereco:</strong> Ary Lievore, 250, Ponta Grossa, Brasil</p>
      <p><strong>E-mail de contato (privacidade):</strong> vitortramontin@gmail.com</p>
      <p><strong>Encarregado (DPO), se aplicavel:</strong> Vitor Marques Tramontin Silveira +5542999770702</p>

      <h2>2. Quais dados coletamos</h2>
      <h3>2.1. Dados cadastrais e de conta</h3>
      <ul>
        <li>Nome, e-mail, senha (armazenada de forma protegida, com hash), telefone (se voce informar)</li>
        <li>Empresa, time, preferencias de uso do dashboard (se aplicavel)</li>
      </ul>

      <h3>2.2. Dados de autenticacao e seguranca</h3>
      <ul>
        <li>Registros de login (data, horario, IP aproximado, dispositivo, navegador)</li>
        <li>Logs tecnicos para auditoria, prevencao de fraude e seguranca</li>
      </ul>

      <h3>2.3. Dados de integracao com plataformas de terceiros (ex: Meta)</h3>
      <p>Se o App permitir integracao com a Meta Platforms (Facebook, Instagram, Meta Ads), podemos tratar:</p>
      <ul>
        <li>IDs e identificadores de contas conectadas (ex: ad account id, page id, ig user id)</li>
        <li>Tokens de acesso fornecidos pelo usuario para permitir a extracao de dados via API</li>
        <li>Dados de campanhas e desempenho (ex: gasto, impressoes, cliques, alcance, metricas de midia)</li>
      </ul>
      <p><strong>Importante:</strong> nao vendemos seus dados, nem usamos tokens para finalidades fora do funcionamento do dashboard.</p>

      <h3>2.4. Dados de uso do App</h3>
      <ul>
        <li>Telas acessadas, acoes realizadas, preferencias, erros e falhas</li>
        <li>Eventos de uso para melhoria de performance e experiencia</li>
      </ul>

      <h3>2.5. Cookies e tecnologias semelhantes (se houver versao web)</h3>
      <ul>
        <li>Cookies essenciais (login, sessao, seguranca)</li>
        <li>Cookies de desempenho e analytics (se voce habilitar, quando aplicavel)</li>
      </ul>

      <h2>3. Para que usamos os dados (finalidades)</h2>
      <p>Usamos os dados para:</p>
      <ul>
        <li>Criar e manter sua conta, autenticar acessos e permitir o uso do App</li>
        <li>Conectar integracoes com terceiros (ex: Meta Graph API) e exibir dados no dashboard</li>
        <li>Sincronizar, consolidar e apresentar metricas e relatorios solicitados por voce</li>
        <li>Garantir seguranca, prevenir fraudes, abuso, acessos nao autorizados</li>
        <li>Melhorar estabilidade, performance, suporte e desenvolvimento de novas funcoes</li>
        <li>Cumprir obrigacoes legais, regulatorias e atender solicitacoes de autoridades, quando exigido</li>
      </ul>

      <h2>4. Bases legais (LGPD)</h2>
      <p>Tratamos dados pessoais com fundamento, conforme aplicavel, em:</p>
      <ul>
        <li>Execucao de contrato, para prestar o servico do App</li>
        <li>Legitimo interesse, para seguranca, prevencao a fraudes e melhoria do servico</li>
        <li>Consentimento, quando necessario (por exemplo, comunicacoes promocionais ou cookies nao essenciais)</li>
        <li>Cumprimento de obrigacao legal ou regulatoria, quando aplicavel</li>
      </ul>

      <h2>5. Compartilhamento de dados</h2>
      <p>Podemos compartilhar dados apenas quando necessario para operar o App, por exemplo:</p>
      <ul>
        <li>Provedores de infraestrutura, hospedagem, banco de dados, armazenamento e monitoramento</li>
        <li>Ferramentas de analytics e logs, para estabilidade e melhoria (quando utilizadas)</li>
        <li>Plataformas integradas, como a Meta, para executar as requisicoes e exibir os dados no dashboard</li>
        <li>Autoridades publicas, mediante obrigacao legal, ordem judicial ou requisicao valida</li>
      </ul>
      <p>Nao comercializamos dados pessoais.</p>

      <h2>6. Integracoes com a Meta (Facebook, Instagram, Ads)</h2>
      <p>Quando voce conecta sua conta, o App pode acessar dados autorizados por voce dentro das permissoes concedidas na plataforma da Meta.</p>
      <ul>
        <li>Tokens e credenciais sao tratados como dados sensiveis de acesso</li>
        <li>Recomendamos que voce mantenha suas credenciais em seguranca</li>
        <li>Voce pode revogar o acesso a qualquer momento pelas configuracoes da sua conta na Meta</li>
        <li>O App usa essas credenciais apenas para consultar e sincronizar dados exibidos no dashboard</li>
      </ul>

      <h2>7. Armazenamento, seguranca e retencao</h2>
      <p>Adotamos medidas de seguranca tecnicas e organizacionais, como:</p>
      <ul>
        <li>Criptografia em transito (HTTPS) e controles de acesso</li>
        <li>Boas praticas de autenticacao, protecao de senha (hash) e segregacao de permissoes</li>
        <li>Monitoramento e logs para deteccao de incidentes</li>
      </ul>
      <p><strong>Retencao:</strong> guardamos dados pelo tempo necessario para operar o servico, cumprir obrigacoes legais, resolver disputas e manter registros de seguranca e auditoria.</p>
      <p>Quando nao for mais necessario, os dados poderao ser excluidos ou anonimizados, salvo obrigacao legal de retencao.</p>

      <h2>8. Transferencia internacional de dados</h2>
      <p>Dependendo dos provedores e integracoes (por exemplo, servicos em nuvem e Meta), seus dados podem ser processados fora do Brasil. Nesses casos, buscamos garantir medidas adequadas de protecao e conformidade com a LGPD.</p>

      <h2>9. Direitos do titular (LGPD)</h2>
      <p>Voce pode solicitar:</p>
      <ul>
        <li>Confirmacao de tratamento e acesso aos dados</li>
        <li>Correcao de dados incompletos, inexatos ou desatualizados</li>
        <li>Anonimizacao, bloqueio ou eliminacao de dados desnecessarios</li>
        <li>Portabilidade, quando aplicavel</li>
        <li>Informacoes sobre compartilhamento</li>
        <li>Revogacao de consentimento, quando o tratamento depender dele</li>
      </ul>
      <p>Para exercer seus direitos, entre em contato em: <strong>vitortramontin@gmail.com</strong>.</p>

      <h2>10. Comunicacoes e notificacoes</h2>
      <p>Podemos enviar comunicacoes relacionadas ao funcionamento do App, como alertas de seguranca, avisos de manutencao e mensagens de suporte.</p>
      <p>Mensagens promocionais, quando existirem, serao enviadas apenas quando permitido, e voce podera optar por nao recebe-las.</p>

      <h2>11. Privacidade de menores</h2>
      <p>O App nao e direcionado a menores de 18 anos, salvo quando autorizado e supervisionado por responsavel legal, conforme aplicavel. Se voce acredita que dados de menor foram tratados indevidamente, contate-nos para avaliacao e providencias.</p>

      <h2>12. Alteracoes desta Politica</h2>
      <p>Podemos atualizar esta Politica para refletir melhorias no App ou exigencias legais. Quando houver mudancas relevantes, vamos informar no App ou por e-mail.</p>

      <h2>13. Contato</h2>
      <p>Em caso de duvidas ou solicitacoes sobre privacidade, fale conosco:</p>
      <ul>
        <li><strong>E-mail:</strong> vitortramontin@gmail.com</li>
        <li><strong>Endereco:</strong> Ary Lievore, 250, Ponta Grossa, Brasil</li>
      </ul>
    </main>
  </body>
</html>"""
    return HttpResponse(html)


@require_GET
def data_deletion(request):
    html = """<!doctype html>
<html lang="pt-BR">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Exclusao de Dados do Usuario</title>
    <style>
      body { font-family: Arial, sans-serif; background: #f4f7fb; color: #102a4d; margin: 0; }
      .container { max-width: 860px; margin: 32px auto; background: #fff; border: 1px solid #c4d5ef; border-radius: 12px; padding: 20px; }
      h1, h2 { color: #082f6e; }
      h1 { margin-top: 0; }
      p { line-height: 1.5; }
      ul { margin-top: 8px; line-height: 1.5; }
    </style>
  </head>
  <body>
    <main class="container">
      <h1>Exclusao de Dados do Usuario</h1>
      <p>Ultima atualizacao: 25/02/2026</p>
      <p>Esta pagina explica como solicitar a exclusao dos dados pessoais tratados pelo Meta Local Dashboard 9.</p>

      <h2>1. Como solicitar a exclusao</h2>
      <p>Voce pode solicitar a exclusao de dados por um dos canais abaixo:</p>
      <ul>
        <li>E-mail: vitortramontin@gmail.com</li>
        <li>Solicitacao direta ao administrador da conta no sistema</li>
      </ul>

      <h2>2. Informacoes recomendadas na solicitacao</h2>
      <ul>
        <li>Nome completo</li>
        <li>E-mail da conta cadastrada</li>
        <li>Identificador da conta (quando disponivel)</li>
        <li>Descricao clara do pedido de exclusao</li>
      </ul>

      <h2>3. Prazo e resposta</h2>
      <p>A solicitacao sera analisada e respondida em prazo razoavel, respeitando obrigacoes legais e de seguranca.</p>

      <h2>4. O que pode permanecer armazenado</h2>
      <p>Alguns registros podem ser mantidos temporariamente quando houver obrigacao legal, prevencao de fraude, auditoria ou defesa em processos.</p>

      <h2>5. Integracoes com terceiros (Meta/Facebook/Instagram)</h2>
      <p>A exclusao dos dados no App nao substitui a revogacao de permissoes diretamente na Meta. Para revogar acesso, acesse as configuracoes da sua conta Meta.</p>

      <h2>6. Contato</h2>
      <p>Em caso de duvidas sobre exclusao de dados: <strong>vitortramontin@gmail.com</strong></p>
    </main>
  </body>
</html>"""
    return HttpResponse(html)
