from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from statistics import mean, pstdev

from Dashboard.services.statistics_utils import (
    linear_trend_with_indexes,
    percent_change,
    round_or_none,
    safe_ratio,
    strict_moving_average,
)


METRIC_CONFIG = {
    'spend': {'label': 'Investimento', 'lower_is_better': False, 'kind': 'currency'},
    'leads': {'label': 'Leads (Resultados)', 'lower_is_better': False, 'kind': 'number'},
    'cpl': {'label': 'CPL (Custo por resultado)', 'lower_is_better': True, 'kind': 'currency'},
    'ctr': {'label': 'CTR', 'lower_is_better': False, 'kind': 'percent'},
    'cpc': {'label': 'CPC', 'lower_is_better': True, 'kind': 'currency'},
    'cpm': {'label': 'CPM', 'lower_is_better': True, 'kind': 'currency'},
    'frequency': {'label': 'Frequência', 'lower_is_better': None, 'kind': 'decimal'},
    'conversions': {'label': 'Conversões (Resultados)', 'lower_is_better': False, 'kind': 'number'},
    'conversion_rate': {'label': 'Taxa de conversão', 'lower_is_better': False, 'kind': 'percent'},
}

ANOMALY_METRICS = (
    'spend',
    'leads',
    'cpl',
    'ctr',
    'cpc',
    'cpm',
    'frequency',
    'conversions',
    'conversion_rate',
)

WEEKDAYS = (
    'segunda-feira',
    'terça-feira',
    'quarta-feira',
    'quinta-feira',
    'sexta-feira',
    'sábado',
    'domingo',
)


def _aggregate_daily(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row['date']].append(row)

    daily_series = []
    for current_date, date_rows in sorted(grouped.items()):
        spend = sum(float(row.get('spend') or 0) for row in date_rows)
        impressions = sum(int(row.get('impressions') or 0) for row in date_rows)
        reach = sum(int(row.get('reach') or 0) for row in date_rows)
        clicks = sum(int(row.get('clicks') or 0) for row in date_rows)
        results = sum(int(row.get('results') or 0) for row in date_rows)
        daily_series.append(
            {
                'date': current_date.isoformat(),
                'spend': round_or_none(spend),
                'impressions': impressions,
                'reach': reach,
                'clicks': clicks,
                'leads': results,
                'conversions': results,
                'ctr': round_or_none(safe_ratio(clicks, impressions)),
                'cpc': round_or_none(safe_ratio(spend, clicks)),
                'cpm': round_or_none(safe_ratio(spend, impressions, 1000)),
                'cpl': round_or_none(safe_ratio(spend, results)),
                'frequency': round_or_none(safe_ratio(impressions, reach)),
                'conversion_rate': round_or_none(safe_ratio(results, clicks)),
            }
        )
    return daily_series


def _valid_values(daily_series, metric):
    return [row[metric] for row in daily_series if row.get(metric) is not None]


def _build_moving_averages(daily_series, metric, warnings):
    values = [row.get(metric) for row in daily_series]
    response = {}
    for window in (3, 7, 14):
        averages = strict_moving_average(values, window)
        if len(values) < window:
            warnings.append(
                f'A média móvel de {window} dias ainda não possui uma janela completa na amostra atual.'
            )
        response[str(window)] = {
            'metric': metric,
            'window': window,
            'points': [
                {
                    'date': row['date'],
                    'value': row.get(metric),
                    'moving_average': averages[index],
                }
                for index, row in enumerate(daily_series)
            ],
        }
    return response


def _trend_strength(slope, values):
    if slope is None or not values:
        return 'indisponível'
    average = abs(mean(values))
    normalized = abs(slope) / average if average else abs(slope)
    if normalized < 0.01:
        return 'estável'
    if normalized < 0.03:
        return 'fraca'
    if normalized < 0.08:
        return 'moderada'
    return 'forte'


def _build_trend(daily_series, metric):
    config = METRIC_CONFIG[metric]
    raw_values = [row.get(metric) for row in daily_series]
    values = [value for value in raw_values if value is not None]
    if len(values) < 3:
        return {
            'available': False,
            'metric': metric,
            'message': 'São necessários pelo menos 3 dias válidos para calcular tendência.',
            'slope': None,
            'direction': 'unavailable',
            'business_direction': 'neutral',
        }

    slope = linear_trend_with_indexes(raw_values)
    average = abs(mean(values))
    stable_threshold = max(average * 0.01, 1e-9)
    direction = 'stable' if abs(slope or 0) <= stable_threshold else 'up' if slope > 0 else 'down'
    if direction == 'stable' or config['lower_is_better'] is None:
        business_direction = 'neutral'
    else:
        improved = direction == 'down' if config['lower_is_better'] else direction == 'up'
        business_direction = 'positive' if improved else 'negative'

    first_value = values[0]
    last_value = values[-1]
    label = config['label']
    direction_text = {'up': 'alta', 'down': 'queda', 'stable': 'estabilidade'}[direction]
    return {
        'available': True,
        'metric': metric,
        'slope': round_or_none(slope),
        'direction': direction,
        'strength': _trend_strength(slope, values),
        'first_value': first_value,
        'last_value': last_value,
        'percent_change': round_or_none(percent_change(last_value, first_value)),
        'business_direction': business_direction,
        'interpretation': f'{label} apresenta tendência de {direction_text} no período selecionado.',
    }


def _weekday_average(rows, key):
    values = [row.get(key) for row in rows if row.get(key) is not None]
    return round_or_none(mean(values)) if values else None


def _build_seasonality(daily_series):
    grouped = defaultdict(list)
    for row in daily_series:
        weekday_number = date.fromisoformat(row['date']).isoweekday()
        grouped[weekday_number].append(row)

    items = []
    for weekday_number, weekday in enumerate(WEEKDAYS, start=1):
        rows = grouped.get(weekday_number, [])
        items.append(
            {
                'weekday': weekday,
                'weekday_number': weekday_number,
                'avg_spend': _weekday_average(rows, 'spend'),
                'avg_leads': _weekday_average(rows, 'leads'),
                'avg_cpl': _weekday_average(rows, 'cpl'),
                'avg_ctr': _weekday_average(rows, 'ctr'),
                'avg_cpc': _weekday_average(rows, 'cpc'),
                'avg_cpm': _weekday_average(rows, 'cpm'),
                'avg_conversions': _weekday_average(rows, 'conversions'),
                'avg_conversion_rate': _weekday_average(rows, 'conversion_rate'),
                'days_count': len(rows),
                'sample_warning': len(rows) < 2,
            }
        )

    reliable = [item for item in items if item['days_count'] >= 2]
    comparable = reliable or [item for item in items if item['days_count']]
    best_cpl = min(
        (item for item in comparable if item['avg_cpl'] is not None),
        key=lambda item: item['avg_cpl'],
        default=None,
    )
    worst_cpl = max(
        (item for item in comparable if item['avg_cpl'] is not None),
        key=lambda item: item['avg_cpl'],
        default=None,
    )
    best_leads = max(
        (item for item in comparable if item['avg_leads'] is not None),
        key=lambda item: item['avg_leads'],
        default=None,
    )
    return {
        'available': bool(daily_series),
        'items': items,
        'best_weekday_by_cpl': best_cpl['weekday'] if best_cpl else None,
        'best_weekday_by_leads': best_leads['weekday'] if best_leads else None,
        'worst_weekday_by_cpl': worst_cpl['weekday'] if worst_cpl else None,
        'sample_warning': len(daily_series) < 21 or not reliable,
        'interpretation': (
            'A sazonalidade fica mais confiável com períodos acima de 21 dias.'
            if len(daily_series) < 21
            else 'Comparação baseada nas médias observadas por dia da semana.'
        ),
    }


def _forecast_metric(daily_series, metric, forecast_days):
    valid = _valid_values(daily_series, metric)
    if len(valid) < 3:
        return {
            'available': False,
            'metric': metric,
            'method': None,
            'points': [],
            'message': 'A previsão precisa de pelo menos 3 dias com dados válidos.',
        }
    recent = valid[-7:]
    predicted = mean(recent)
    deviation = pstdev(recent) if len(recent) > 1 else 0.0
    last_date = date.fromisoformat(daily_series[-1]['date'])
    is_additive = metric in {'spend', 'leads', 'conversions'}
    projected_value = predicted * forecast_days if is_additive else predicted
    interpretation = (
        f'Com base na média recente, a projeção é de aproximadamente '
        f'{round_or_none(projected_value)} em {forecast_days} dias.'
        if is_additive
        else (
            f'Com base na média recente, o valor diário projetado para os próximos '
            f'{forecast_days} dias é {round_or_none(predicted)}.'
        )
    )
    points = [
        {
            'date': (last_date + timedelta(days=index)).isoformat(),
            'predicted_value': round_or_none(predicted),
            'lower_bound': round_or_none(max(0.0, predicted - deviation)),
            'upper_bound': round_or_none(predicted + deviation),
        }
        for index in range(1, forecast_days + 1)
    ]
    return {
        'available': True,
        'metric': metric,
        'method': 'moving_average_7d',
        'confidence': 'low' if len(valid) < 7 else 'medium' if len(valid) < 14 else 'high',
        'sample_size': len(valid),
        'daily_average': round_or_none(predicted),
        'projected_total': round_or_none(projected_value),
        'points': points,
        'interpretation': interpretation,
    }


def _build_forecast(daily_series, selected_metric, forecast_days):
    metrics = {
        metric: _forecast_metric(daily_series, metric, forecast_days)
        for metric in ('spend', 'leads', 'cpl', 'conversions')
    }
    selected = metrics.get(selected_metric) or _forecast_metric(daily_series, selected_metric, forecast_days)
    return {
        'available': selected['available'],
        'forecast_days': forecast_days,
        'metric': selected_metric,
        'method': selected.get('method'),
        'confidence': selected.get('confidence'),
        'points': selected.get('points', []),
        'projected_total': selected.get('projected_total'),
        'interpretation': selected.get('interpretation') or selected.get('message'),
        'metrics': metrics,
    }


def _build_goal_projection(daily_series, goal_leads):
    if goal_leads is None:
        return {'available': False, 'goal_leads': None, 'message': 'Informe uma meta para estimar o investimento.'}
    cpl_values = _valid_values(daily_series, 'cpl')
    if not cpl_values:
        return {
            'available': False,
            'goal_leads': goal_leads,
            'message': 'Não foi possível calcular CPL porque não há resultados no período selecionado.',
        }
    recent = cpl_values[-7:]
    average = mean(recent)
    deviation = pstdev(recent) if len(recent) > 1 else 0.0
    optimistic_cpl = max(0.0, average - deviation)
    return {
        'available': True,
        'goal_leads': round_or_none(goal_leads),
        'recent_avg_cpl': round_or_none(average),
        'estimated_required_spend': round_or_none(goal_leads * average),
        'confidence': 'low' if len(cpl_values) < 7 else 'medium' if len(cpl_values) < 14 else 'high',
        'scenarios': {
            'optimistic': round_or_none(goal_leads * optimistic_cpl),
            'base': round_or_none(goal_leads * average),
            'conservative': round_or_none(goal_leads * (average + deviation)),
        },
        'interpretation': (
            f'Para gerar {round_or_none(goal_leads)} resultados, o investimento estimado é de '
            f'R$ {goal_leads * average:,.2f} com base no CPL recente.'
        ),
    }


def _anomaly_interpretation(metric, value, average):
    config = METRIC_CONFIG[metric]
    relation = 'acima' if value > average else 'abaixo'
    if config['lower_is_better'] is True:
        impact = 'sinal negativo' if value > average else 'sinal favorável'
    elif config['lower_is_better'] is False:
        impact = 'sinal favorável' if value > average else 'sinal negativo'
    else:
        impact = 'ponto de atenção'
    return f'{config["label"]} ficou muito {relation} do padrão do período ({impact}).'


def _build_anomalies(daily_series):
    if len(daily_series) < 7:
        return []
    anomalies = []
    for metric in ANOMALY_METRICS:
        values = _valid_values(daily_series, metric)
        if len(values) < 7:
            continue
        average = mean(values)
        deviation = pstdev(values)
        if deviation == 0:
            continue
        for row in daily_series:
            value = row.get(metric)
            if value is None:
                continue
            z_score = (value - average) / deviation
            if abs(z_score) < 2.5:
                continue
            anomalies.append(
                {
                    'date': row['date'],
                    'metric': metric,
                    'metric_label': METRIC_CONFIG[metric]['label'],
                    'value': value,
                    'mean': round_or_none(average),
                    'std_dev': round_or_none(deviation),
                    'z_score': round_or_none(z_score),
                    'severity': 'high' if abs(z_score) >= 3 else 'moderate',
                    'business_direction': (
                        'negative'
                        if (
                            METRIC_CONFIG[metric]['lower_is_better'] is True and value > average
                        ) or (
                            METRIC_CONFIG[metric]['lower_is_better'] is False and value < average
                        )
                        else 'neutral'
                    ),
                    'interpretation': _anomaly_interpretation(metric, value, average),
                }
            )
    return sorted(anomalies, key=lambda item: abs(item['z_score']), reverse=True)


def _build_summary(daily_series, metric, trend, moving_averages, seasonality, forecast, anomalies):
    values = _valid_values(daily_series, metric)
    ma7_points = moving_averages['7']['points']
    latest_ma7 = next(
        (point['moving_average'] for point in reversed(ma7_points) if point['moving_average'] is not None),
        None,
    )
    return {
        'sample_size': len(daily_series),
        'valid_metric_points': len(values),
        'average': round_or_none(mean(values)) if values else None,
        'current_value': values[-1] if values else None,
        'moving_average_7d': latest_ma7,
        'trend_direction': trend.get('direction'),
        'business_direction': trend.get('business_direction'),
        'best_weekday': (
            seasonality.get('best_weekday_by_cpl')
            if metric == 'cpl'
            else seasonality.get('best_weekday_by_leads')
        ),
        'anomalies_count': len(anomalies),
        'forecast_total': forecast.get('projected_total'),
    }


def _build_insights(metric, trend, seasonality, forecast, goal_projection, anomalies):
    insights = []
    if trend.get('available'):
        insight_type = {
            'positive': 'success',
            'negative': 'warning',
            'neutral': 'info',
        }[trend['business_direction']]
        insights.append(
            {
                'type': insight_type,
                'title': f'Tendência de {METRIC_CONFIG[metric]["label"]}',
                'description': trend['interpretation'],
                'evidence': [
                    f'Valor inicial: {trend["first_value"]}',
                    f'Valor final: {trend["last_value"]}',
                    f'Inclinação: {trend["slope"]}',
                ],
                'suggested_action': 'Monitorar a continuidade da tendência antes de alterar a estratégia.',
            }
        )
    if seasonality.get('best_weekday_by_cpl'):
        insights.append(
            {
                'type': 'success',
                'title': 'Melhor eficiência semanal observada',
                'description': (
                    f'{seasonality["best_weekday_by_cpl"].capitalize()} apresentou o menor CPL médio.'
                ),
                'evidence': ['Comparação pelas médias dos dias disponíveis no período.'],
                'suggested_action': 'Avaliar uma redistribuição gradual de verba, sem tratar a associação como causal.',
            }
        )
    if anomalies:
        strongest = anomalies[0]
        insights.append(
            {
                'type': 'danger',
                'title': 'Anomalia detectada',
                'description': strongest['interpretation'],
                'evidence': [
                    f'Data: {strongest["date"]}',
                    f'Z-score: {strongest["z_score"]}',
                ],
                'suggested_action': 'Investigar mudanças de orçamento, criativo, público ou sincronização nesse dia.',
            }
        )
    if forecast.get('available'):
        insights.append(
            {
                'type': 'info',
                'title': 'Projeção operacional',
                'description': forecast['interpretation'],
                'evidence': [f'Método: {forecast["method"]}', f'Confiança: {forecast["confidence"]}'],
                'suggested_action': 'Usar a projeção como referência de planejamento, não como garantia.',
            }
        )
    if goal_projection.get('available'):
        insights.append(
            {
                'type': 'info',
                'title': 'Investimento estimado para a meta',
                'description': goal_projection['interpretation'],
                'evidence': [
                    f'Cenário base: {goal_projection["scenarios"]["base"]}',
                    f'Cenário conservador: {goal_projection["scenarios"]["conservative"]}',
                ],
                'suggested_action': 'Revisar a estimativa conforme o CPL real evoluir.',
            }
        )
    return insights


def build_time_series_analysis(*, rows, date_start, date_end, metric, forecast_days, goal_leads):
    daily_series = _aggregate_daily(rows)
    warnings = []
    sample_size = len(daily_series)
    if not daily_series:
        warnings.append('Não há dados diários suficientes para análise temporal.')
    elif sample_size < 3:
        warnings.append('Menos de 3 dias: apenas a série diária possui base suficiente.')
    elif sample_size < 7:
        warnings.append('A previsão possui baixa confiabilidade e anomalias não serão calculadas.')
    elif sample_size < 14:
        warnings.append('A amostra permite forecast, mas ainda possui baixa confiança.')
    elif sample_size < 21:
        warnings.append('A sazonalidade fica mais confiável com períodos acima de 21 dias.')

    moving_averages = _build_moving_averages(daily_series, metric, warnings)
    trend = _build_trend(daily_series, metric)
    seasonality = _build_seasonality(daily_series)
    forecast = _build_forecast(daily_series, metric, forecast_days)
    if 0 < sample_size < 7:
        warnings.append('O forecast usa menos de 7 pontos válidos e deve ser interpretado com cautela.')
    goal_projection = _build_goal_projection(daily_series, goal_leads)
    if goal_leads is not None and 0 < len(_valid_values(daily_series, 'cpl')) < 7:
        warnings.append('A projeção de investimento usa o CPL médio do período por falta de 7 dias válidos.')
    anomalies = _build_anomalies(daily_series)
    if 0 < sample_size < 7:
        warnings.append('Anomalias exigem pelo menos 7 dias válidos.')

    summary = _build_summary(
        daily_series,
        metric,
        trend,
        moving_averages,
        seasonality,
        forecast,
        anomalies,
    )
    return {
        'period': {
            'date_start': date_start.isoformat(),
            'date_end': date_end.isoformat(),
            'days': (date_end - date_start).days + 1,
            'days_with_data': sample_size,
        },
        'metric': metric,
        'meta': {
            'metric_label': METRIC_CONFIG[metric]['label'],
            'metric_kind': METRIC_CONFIG[metric]['kind'],
            'result_semantics': (
                'Leads e conversões usam Resultados como proxy do objetivo configurado na campanha; '
                'não representam necessariamente leads qualificados ou vendas.'
            ),
        },
        'summary': summary,
        'daily_series': daily_series,
        'moving_averages': moving_averages,
        'trend': trend,
        'seasonality': seasonality,
        'forecast': forecast,
        'goal_projection': goal_projection,
        'anomalies': anomalies,
        'insights': _build_insights(metric, trend, seasonality, forecast, goal_projection, anomalies),
        'warnings': list(dict.fromkeys(warnings)),
    }
