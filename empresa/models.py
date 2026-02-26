from datetime import timedelta
from decimal import Decimal

from django.db import models
from django.utils import timezone


class Cliente(models.Model):
    FORMA_PAGAMENTO_PIX = 'PIX'
    FORMA_PAGAMENTO_CARTAO_CREDITO = 'CARTAO CREDITO'
    FORMA_PAGAMENTO_CHOICES = [
        (FORMA_PAGAMENTO_PIX, 'PIX'),
        (FORMA_PAGAMENTO_CARTAO_CREDITO, 'CARTAO CREDITO'),
    ]
    PERIODO_COBRANCA_SEMANAL = 'SEMANAL'
    PERIODO_COBRANCA_MENSAL = 'MENSAL'
    PERIODO_COBRANCA_CHOICES = [
        (PERIODO_COBRANCA_SEMANAL, 'SEMANAL'),
        (PERIODO_COBRANCA_MENSAL, 'MENSAL'),
    ]

    name = models.CharField(max_length=255, default='', db_index=True)
    nicho_atuacao = models.CharField(max_length=255, default='', blank=True)
    valor_investido = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    forma_pagamento = models.CharField(
        max_length=20,
        choices=FORMA_PAGAMENTO_CHOICES,
        default=FORMA_PAGAMENTO_PIX,
    )
    periodo_cobranca = models.CharField(
        max_length=20,
        choices=PERIODO_COBRANCA_CHOICES,
        default=PERIODO_COBRANCA_MENSAL,
    )
    saldo_atual = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    gasto_diario = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    nome = models.ForeignKey(
        'Dashboard.AdAccount',
        on_delete=models.CASCADE,
        related_name='clientes',
    )
    data_renovacao_creditos = models.DateField()
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f'{self.name} ({self.nome.name}) - renovacao {self.data_renovacao_creditos}'

    def calcular_data_renovacao_creditos(self, *, base_date=None):
        today = base_date or timezone.localdate()
        saldo = Decimal(self.saldo_atual or 0)
        gasto = Decimal(self.gasto_diario or 0)

        if gasto <= 0:
            dias = 0
        else:
            dias = int((saldo / gasto) - Decimal('2'))

        return today + timedelta(days=dias)

    def save(self, *args, **kwargs):
        self.data_renovacao_creditos = self.calcular_data_renovacao_creditos()
        update_fields = kwargs.get('update_fields')
        if update_fields is not None:
            merged_update_fields = set(update_fields)
            merged_update_fields.add('data_renovacao_creditos')
            kwargs['update_fields'] = list(merged_update_fields)
        super().save(*args, **kwargs)
