import json

from django.contrib.auth import authenticate, login, logout
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_protect, ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST


@ensure_csrf_cookie
@require_GET
def auth_me(request):
    if not request.user.is_authenticated:
        return JsonResponse({'authenticated': False, 'user': None}, status=200)

    return JsonResponse(
        {
            'authenticated': True,
            'user': {
                'id': request.user.id,
                'username': request.user.username,
                'email': request.user.email,
            },
        },
        status=200,
    )


@csrf_protect
@require_POST
def auth_login(request):
    try:
        payload = json.loads(request.body or '{}')
    except json.JSONDecodeError:
        return JsonResponse({'detail': 'JSON invalido.'}, status=400)

    username = (payload.get('username') or '').strip()
    password = payload.get('password') or ''

    if not username or not password:
        return JsonResponse({'detail': 'username e password sao obrigatorios.'}, status=400)

    user = authenticate(request, username=username, password=password)
    if user is None:
        return JsonResponse({'detail': 'Credenciais invalidas.'}, status=401)

    login(request, user)
    return JsonResponse(
        {
            'authenticated': True,
            'user': {
                'id': user.id,
                'username': user.username,
                'email': user.email,
            },
        },
        status=200,
    )


@csrf_protect
@require_POST
def auth_logout(request):
    if request.user.is_authenticated:
        logout(request)
    return JsonResponse({'authenticated': False}, status=200)
