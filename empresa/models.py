from django.db import models


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
