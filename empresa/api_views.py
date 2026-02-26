import logging
from decimal import Decimal, InvalidOperation

from django.utils.dateparse import parse_date
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from Dashboard.models import AdAccount

from .models import Cliente
from .meta_funding_service import sync_clientes_saldo_atual_from_meta


logger = logging.getLogger(__name__)


def _parse_ids_param(raw_ids: str):
    values = []
    seen = set()
    for chunk in raw_ids.split(','):
        item = chunk.strip()
        if not item:
            continue
        if not item.isdigit():
            return None
        parsed = int(item)
        if parsed <= 0:
            return None
        if parsed in seen:
            continue
        seen.add(parsed)
        values.append(parsed)
    return values


def _parse_decimal_field(raw_value, field_name: str):
    text = str(raw_value or '').strip()
    if not text:
        return Decimal('0'), None
    if ',' in text and '.' not in text:
        text = text.replace(',', '.')
    try:
        return Decimal(text), None
    except (InvalidOperation, ValueError):
        return None, f'Campo {field_name} invalido. Informe um numero decimal valido, ex: 1234.56.'


def _parse_forma_pagamento(raw_value):
    value = str(raw_value or '').strip().upper()
    valid_values = {
        Cliente.FORMA_PAGAMENTO_PIX,
        Cliente.FORMA_PAGAMENTO_CARTAO_CREDITO,
    }
    if value not in valid_values:
        return None, 'Campo forma_pagamento invalido. Valores permitidos: PIX, CARTAO CREDITO.'
    return value, None


def _parse_periodo_cobranca(raw_value):
    value = str(raw_value or '').strip().upper()
    valid_values = {
        Cliente.PERIODO_COBRANCA_SEMANAL,
        Cliente.PERIODO_COBRANCA_MENSAL,
    }
    if value not in valid_values:
        return None, 'Campo periodo_cobranca invalido. Valores permitidos: SEMANAL, MENSAL.'
    return value, None


def _serialize_cliente(cliente: Cliente) -> dict:
    return {
        'id': cliente.id,
        'name': cliente.name,
        'nicho_atuacao': cliente.nicho_atuacao,
        'valor_investido': cliente.valor_investido,
        'forma_pagamento': cliente.forma_pagamento,
        'periodo_cobranca': cliente.periodo_cobranca,
        'saldo_atual': cliente.saldo_atual,
        'gasto_diario': cliente.gasto_diario,
        'nome_id': cliente.nome_id,
        'nome': cliente.nome.name,
        'id_meta_ad_account': cliente.nome.id_meta_ad_account,
        'data_renovacao_creditos': cliente.data_renovacao_creditos,
        'created_at': cliente.created_at,
        'updated_at': cliente.updated_at,
    }


@api_view(['GET', 'POST', 'DELETE'])
@permission_classes([IsAuthenticated])
def clientes(request):
    if request.method == 'GET':
        refresh_saldo_raw = str(request.query_params.get('refresh_saldo') or '').strip().lower()
        should_refresh_saldo = refresh_saldo_raw in {'1', 'true', 'yes'}
        saldo_sync = None
        if should_refresh_saldo:
            try:
                saldo_sync = sync_clientes_saldo_atual_from_meta(request.user)
            except Exception:
                logger.exception('Falha inesperada ao sincronizar saldo_atual dos clientes.')
                saldo_sync = {
                    'updated_clientes': 0,
                    'total_clientes': 0,
                    'total_ad_accounts': 0,
                    'error_count': 1,
                    'parse_error_count': 0,
                    'skipped': False,
                    'detail': 'Falha inesperada ao sincronizar saldo_atual.',
                }

        queryset = (
            Cliente.objects.select_related('nome')
            .filter(nome__id_dashboard_user__user=request.user)
            .order_by('-created_at')
        )
        raw_ids = str(request.query_params.get('ids') or '').strip()
        if raw_ids:
            parsed_ids = _parse_ids_param(raw_ids)
            if parsed_ids is None:
                return Response(
                    {'detail': 'Parametro ids invalido. Use numeros separados por virgula, ex: ids=1,2,3.'},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            queryset = queryset.filter(id__in=parsed_ids)

        payload = [_serialize_cliente(cliente) for cliente in queryset]
        response_payload = {'clientes': payload, 'total': len(payload)}
        if saldo_sync is not None:
            response_payload['saldo_sync'] = saldo_sync
        return Response(response_payload, status=status.HTTP_200_OK)

    if request.method == 'DELETE':
        raw_ids = str(request.query_params.get('ids') or '').strip()
        if not raw_ids:
            return Response(
                {'detail': 'Parametro ids e obrigatorio para exclusao. Use: ids=1,2,3.'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        parsed_ids = _parse_ids_param(raw_ids)
        if parsed_ids is None:
            return Response(
                {'detail': 'Parametro ids invalido. Use numeros separados por virgula, ex: ids=1,2,3.'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        queryset = Cliente.objects.filter(
            id__in=parsed_ids,
            nome__id_dashboard_user__user=request.user,
        )
        deleted_count, _ = queryset.delete()

        return Response(
            {
                'detail': 'Clientes excluidos com sucesso.',
                'deleted_count': int(deleted_count),
                'requested_ids': parsed_ids,
            },
            status=status.HTTP_200_OK,
        )

    cliente_name = str(request.data.get('name') or '').strip()
    if not cliente_name:
        return Response(
            {'detail': 'Campo name e obrigatorio.'},
            status=status.HTTP_400_BAD_REQUEST,
        )

    ad_account_id_raw = request.data.get('nome')
    if ad_account_id_raw in (None, ''):
        return Response(
            {'detail': 'Campo nome e obrigatorio e deve conter o ID interno de AdAccount.'},
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        ad_account_id = int(ad_account_id_raw)
    except (TypeError, ValueError):
        return Response(
            {'detail': 'Campo nome invalido. Informe o ID interno numerico de AdAccount.'},
            status=status.HTTP_400_BAD_REQUEST,
        )

    data_renovacao_raw = str(request.data.get('data_renovacao_creditos') or '').strip()
    data_renovacao = parse_date(data_renovacao_raw)
    if data_renovacao is None:
        return Response(
            {'detail': 'Campo data_renovacao_creditos invalido. Use o formato YYYY-MM-DD.'},
            status=status.HTTP_400_BAD_REQUEST,
        )

    ad_account = AdAccount.objects.filter(
        id=ad_account_id,
        id_dashboard_user__user=request.user,
    ).first()
    if ad_account is None:
        return Response(
            {'detail': 'AdAccount nao encontrado para o usuario autenticado.'},
            status=status.HTTP_404_NOT_FOUND,
        )

    nicho_atuacao = str(request.data.get('nicho_atuacao') or '').strip()
    forma_pagamento, forma_pagamento_error = _parse_forma_pagamento(request.data.get('forma_pagamento'))
    if forma_pagamento_error is not None:
        return Response({'detail': forma_pagamento_error}, status=status.HTTP_400_BAD_REQUEST)
    periodo_cobranca, periodo_cobranca_error = _parse_periodo_cobranca(request.data.get('periodo_cobranca'))
    if periodo_cobranca_error is not None:
        return Response({'detail': periodo_cobranca_error}, status=status.HTTP_400_BAD_REQUEST)

    valor_investido, valor_investido_error = _parse_decimal_field(
        request.data.get('valor_investido'),
        'valor_investido',
    )
    if valor_investido_error is not None:
        return Response({'detail': valor_investido_error}, status=status.HTTP_400_BAD_REQUEST)

    saldo_atual, saldo_atual_error = _parse_decimal_field(request.data.get('saldo_atual'), 'saldo_atual')
    if saldo_atual_error is not None:
        return Response({'detail': saldo_atual_error}, status=status.HTTP_400_BAD_REQUEST)

    gasto_diario, gasto_diario_error = _parse_decimal_field(request.data.get('gasto_diario'), 'gasto_diario')
    if gasto_diario_error is not None:
        return Response({'detail': gasto_diario_error}, status=status.HTTP_400_BAD_REQUEST)

    cliente = Cliente.objects.create(
        name=cliente_name,
        nicho_atuacao=nicho_atuacao,
        valor_investido=valor_investido,
        forma_pagamento=forma_pagamento,
        periodo_cobranca=periodo_cobranca,
        saldo_atual=saldo_atual,
        gasto_diario=gasto_diario,
        nome=ad_account,
        data_renovacao_creditos=data_renovacao,
    )
    return Response(
        {
            'detail': 'Cliente cadastrado com sucesso.',
            'cliente': _serialize_cliente(cliente),
        },
        status=status.HTTP_201_CREATED,
    )


@api_view(['PATCH'])
@permission_classes([IsAuthenticated])
def cliente_detail(request, cliente_id: int):
    cliente = (
        Cliente.objects.select_related('nome')
        .filter(id=cliente_id, nome__id_dashboard_user__user=request.user)
        .first()
    )
    if cliente is None:
        return Response(
            {'detail': 'Cliente nao encontrado para o usuario autenticado.'},
            status=status.HTTP_404_NOT_FOUND,
        )

    has_updates = False

    if 'name' in request.data:
        cliente_name = str(request.data.get('name') or '').strip()
        if not cliente_name:
            return Response(
                {'detail': 'Campo name e obrigatorio.'},
                status=status.HTTP_400_BAD_REQUEST,
            )
        cliente.name = cliente_name
        has_updates = True

    if 'nome' in request.data:
        ad_account_id_raw = request.data.get('nome')
        if ad_account_id_raw in (None, ''):
            return Response(
                {'detail': 'Campo nome e obrigatorio e deve conter o ID interno de AdAccount.'},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            ad_account_id = int(ad_account_id_raw)
        except (TypeError, ValueError):
            return Response(
                {'detail': 'Campo nome invalido. Informe o ID interno numerico de AdAccount.'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        ad_account = AdAccount.objects.filter(
            id=ad_account_id,
            id_dashboard_user__user=request.user,
        ).first()
        if ad_account is None:
            return Response(
                {'detail': 'AdAccount nao encontrado para o usuario autenticado.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        cliente.nome = ad_account
        has_updates = True

    if 'data_renovacao_creditos' in request.data:
        data_renovacao_raw = str(request.data.get('data_renovacao_creditos') or '').strip()
        data_renovacao = parse_date(data_renovacao_raw)
        if data_renovacao is None:
            return Response(
                {'detail': 'Campo data_renovacao_creditos invalido. Use o formato YYYY-MM-DD.'},
                status=status.HTTP_400_BAD_REQUEST,
            )
        cliente.data_renovacao_creditos = data_renovacao
        has_updates = True

    if 'nicho_atuacao' in request.data:
        cliente.nicho_atuacao = str(request.data.get('nicho_atuacao') or '').strip()
        has_updates = True

    if 'forma_pagamento' in request.data:
        forma_pagamento, forma_pagamento_error = _parse_forma_pagamento(request.data.get('forma_pagamento'))
        if forma_pagamento_error is not None:
            return Response({'detail': forma_pagamento_error}, status=status.HTTP_400_BAD_REQUEST)
        cliente.forma_pagamento = forma_pagamento
        has_updates = True

    if 'periodo_cobranca' in request.data:
        periodo_cobranca, periodo_cobranca_error = _parse_periodo_cobranca(request.data.get('periodo_cobranca'))
        if periodo_cobranca_error is not None:
            return Response({'detail': periodo_cobranca_error}, status=status.HTTP_400_BAD_REQUEST)
        cliente.periodo_cobranca = periodo_cobranca
        has_updates = True

    if 'valor_investido' in request.data:
        valor_investido, valor_investido_error = _parse_decimal_field(
            request.data.get('valor_investido'),
            'valor_investido',
        )
        if valor_investido_error is not None:
            return Response({'detail': valor_investido_error}, status=status.HTTP_400_BAD_REQUEST)
        cliente.valor_investido = valor_investido
        has_updates = True

    if 'saldo_atual' in request.data:
        saldo_atual, saldo_atual_error = _parse_decimal_field(request.data.get('saldo_atual'), 'saldo_atual')
        if saldo_atual_error is not None:
            return Response({'detail': saldo_atual_error}, status=status.HTTP_400_BAD_REQUEST)
        cliente.saldo_atual = saldo_atual
        has_updates = True

    if 'gasto_diario' in request.data:
        gasto_diario, gasto_diario_error = _parse_decimal_field(request.data.get('gasto_diario'), 'gasto_diario')
        if gasto_diario_error is not None:
            return Response({'detail': gasto_diario_error}, status=status.HTTP_400_BAD_REQUEST)
        cliente.gasto_diario = gasto_diario
        has_updates = True

    if not has_updates:
        return Response(
            {'detail': 'Nenhum campo valido enviado para atualizacao.'},
            status=status.HTTP_400_BAD_REQUEST,
        )

    cliente.save()
    cliente.refresh_from_db()
    return Response(
        {
            'detail': 'Cliente atualizado com sucesso.',
            'cliente': _serialize_cliente(cliente),
        },
        status=status.HTTP_200_OK,
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def empresa_ad_accounts(request):
    ad_accounts = (
        AdAccount.objects.filter(id_dashboard_user__user=request.user)
        .order_by('name')
        .values('id', 'name', 'id_meta_ad_account')
    )
    payload = list(ad_accounts)
    return Response({'ad_accounts': payload, 'total': len(payload)}, status=status.HTTP_200_OK)
