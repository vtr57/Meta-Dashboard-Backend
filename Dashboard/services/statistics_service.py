from __future__ import annotations

from collections import defaultdict

from Dashboard.services.statistics_utils import (
    correlation_strength,
    descriptive_statistics,
    linear_trend_slope,
    moving_average,
    pearson_correlation,
    percent_change,
    round_or_none,
    safe_ratio,
    two_proportion_z_test,
    welch_mean_test,
    z_scores,
)


METRIC_CONFIG = {
    'spend': {'label': 'Investimento', 'lower_is_better': False},
    'impressions': {'label': 'Impressões', 'lower_is_better': False},
    'reach': {'label': 'Alcance', 'lower_is_better': False},
    'clicks': {'label': 'Cliques', 'lower_is_better': False},
    'ctr': {'label': 'CTR', 'lower_is_better': False},
    'cpc': {'label': 'CPC', 'lower_is_better': True},
    'cpm': {'label': 'CPM', 'lower_is_better': True},
    'results': {'label': 'Resultados', 'lower_is_better': False},
    'cost_per_result': {'label': 'Custo por resultado', 'lower_is_better': True},
    'frequency': {'label': 'Frequência', 'lower_is_better': None},
}

STABILITY_METRICS = ('spend', 'results', 'ctr', 'cpc', 'cost_per_result')
TREND_METRICS = ('spend', 'results', 'ctr', 'cpc', 'cpm', 'cost_per_result')


def _daily_metrics(row):
    spend = float(row.get('spend') or 0)
    impressions = int(row.get('impressions') or 0)
    reach = int(row.get('reach') or 0)
    results = int(row.get('results') or 0)
    clicks = int(row.get('clicks') or 0)
    return {
        **row,
        'spend': spend,
        'impressions': impressions,
        'reach': reach,
        'results': results,
        'clicks': clicks,
        'ctr': safe_ratio(clicks, impressions, 100, 0.0),
        'cpc': safe_ratio(spend, clicks),
        'cpm': safe_ratio(spend, impressions, 1000),
        'cost_per_result': safe_ratio(spend, results),
        'frequency': safe_ratio(impressions, reach),
    }


def _normalize_rows(rows):
    return [_daily_metrics(row) for row in rows]


def _aggregate(rows):
    spend = sum(row['spend'] for row in rows)
    impressions = sum(row['impressions'] for row in rows)
    reach = sum(row['reach'] for row in rows)
    results = sum(row['results'] for row in rows)
    clicks = sum(row['clicks'] for row in rows)
    return {
        'spend': round_or_none(spend),
        'impressions': impressions,
        'reach': reach,
        'clicks': clicks,
        'ctr': round_or_none(safe_ratio(clicks, impressions, 100, 0.0)),
        'cpc': round_or_none(safe_ratio(spend, clicks)),
        'cpm': round_or_none(safe_ratio(spend, impressions, 1000)),
        'results': results,
        'cost_per_result': round_or_none(safe_ratio(spend, results)),
        'frequency': round_or_none(safe_ratio(impressions, reach)),
    }


def _group_by_date(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[str(row['date'])].append(row)
    return [
        {'date': date, **_aggregate(date_rows)}
        for date, date_rows in sorted(grouped.items())
    ]


def _group_by_entity(rows):
    grouped_by_date = defaultdict(lambda: defaultdict(list))
    names = {}
    for row in rows:
        entity_id = str(row.get('entity_id') or '')
        grouped_by_date[entity_id][str(row['date'])].append(row)
        names[entity_id] = row.get('entity_name') or entity_id
    grouped = {}
    for entity_id, dates in grouped_by_date.items():
        grouped[entity_id] = [
            {'date': date, 'entity_id': entity_id, 'entity_name': names[entity_id], **_aggregate(date_rows)}
            for date, date_rows in sorted(dates.items())
        ]
    return grouped, names


def _metric_direction(config, current, previous):
    change = percent_change(current, previous)
    if change is None:
        return 'sem_comparacao'
    if change == 0 or config['lower_is_better'] is None:
        return 'neutral'
    improved = change < 0 if config['lower_is_better'] else change > 0
    return 'positive' if improved else 'negative'


def _metric_interpretation(label, direction, change):
    if change is None:
        return f'Não há base anterior suficiente para comparar {label}.'
    if direction == 'neutral':
        return f'{label} permaneceu praticamente estável no período.'
    movement = 'melhorou' if direction == 'positive' else 'piorou'
    return f'{label} {movement} {abs(change):.2f}% em relação ao período anterior.'


def build_overview(current_rows, previous_rows, compare):
    current = _aggregate(current_rows)
    previous = _aggregate(previous_rows) if compare and previous_rows else {}
    metrics = []
    for key, config in METRIC_CONFIG.items():
        current_value = current.get(key)
        previous_value = previous.get(key) if compare and previous_rows else None
        absolute_change = (
            round_or_none(current_value - previous_value)
            if current_value is not None and previous_value is not None
            else None
        )
        change = round_or_none(percent_change(current_value, previous_value))
        direction = _metric_direction(config, current_value, previous_value)
        metrics.append(
            {
                'metric': key,
                'label': config['label'],
                'current_value': current_value,
                'previous_value': previous_value,
                'absolute_change': absolute_change,
                'percent_change': change,
                'direction': direction,
                'interpretation': _metric_interpretation(config['label'], direction, change),
            }
        )
    return {
        'available': bool(current_rows),
        'message': '' if current_rows else 'Não há dados de mídia no período selecionado.',
        'metrics': metrics,
    }


def build_stability(rows, entity_type):
    grouped, names = _group_by_entity(rows)
    items = []
    for entity_id, entity_rows in grouped.items():
        ordered_rows = sorted(entity_rows, key=lambda row: str(row['date']))
        zero_result_days = sum(1 for row in ordered_rows if row['results'] == 0)
        for metric in STABILITY_METRICS:
            valid_rows = [row for row in ordered_rows if row.get(metric) is not None]
            stats = descriptive_statistics([row[metric] for row in valid_rows])
            lower_is_better = METRIC_CONFIG[metric]['lower_is_better']
            best_row = None
            worst_row = None
            if valid_rows:
                best_row = min(valid_rows, key=lambda row: row[metric]) if lower_is_better else max(
                    valid_rows, key=lambda row: row[metric]
                )
                worst_row = max(valid_rows, key=lambda row: row[metric]) if lower_is_better else min(
                    valid_rows, key=lambda row: row[metric]
                )
            items.append(
                {
                    'entity_type': entity_type,
                    'entity_id': entity_id,
                    'entity_name': names[entity_id],
                    'metric': metric,
                    'metric_label': METRIC_CONFIG[metric]['label'],
                    **stats,
                    'zero_result_days': zero_result_days,
                    'best_day': str(best_row['date']) if best_row else None,
                    'worst_day': str(worst_row['date']) if worst_row else None,
                    'interpretation': (
                        f'{names[entity_id]} está {stats["stability_label"]} em {METRIC_CONFIG[metric]["label"]}.'
                    ),
                }
            )
    items.sort(
        key=lambda item: (
            item['coefficient_of_variation'] is None,
            -(item['coefficient_of_variation'] or 0),
            item['entity_name'],
        )
    )
    return {
        'available': bool(items),
        'message': '' if items else 'Ainda não há dados suficientes para calcular estabilidade.',
        'items': items[:80],
    }


def build_funnel(rows):
    totals = _aggregate(rows)
    steps = [
        {'key': 'impressions', 'label': 'Impressões', 'value': totals['impressions'], 'available': True},
        {'key': 'clicks', 'label': 'Cliques', 'value': totals['clicks'], 'available': True},
        {
            'key': 'results',
            'label': 'Resultados',
            'value': totals['results'],
            'available': True,
            'note': 'Proxy do objetivo configurado na campanha; pode representar lead ou mensagem.',
        },
        {'key': 'qualified_leads', 'label': 'Leads qualificados', 'value': None, 'available': False},
        {'key': 'proposals', 'label': 'Propostas', 'value': None, 'available': False},
        {'key': 'sales', 'label': 'Vendas', 'value': None, 'available': False},
    ]
    return {
        'available': bool(rows),
        'message': (
            'Esta análise usa apenas dados de mídia; dados comerciais não estão disponíveis.'
            if rows
            else 'Não há dados suficientes para montar o funil.'
        ),
        'steps': steps,
        'rates': [
            {
                'key': 'ctr',
                'label': 'Impressão → clique',
                'value': round_or_none(safe_ratio(totals['clicks'], totals['impressions'], 100)),
                'available': totals['impressions'] > 0,
            },
            {
                'key': 'click_to_result',
                'label': 'Clique → resultado',
                'value': round_or_none(safe_ratio(totals['results'], totals['clicks'], 100)),
                'available': totals['clicks'] > 0,
            },
        ],
        'costs': [
            {
                'key': 'cost_per_result',
                'label': 'Custo por resultado',
                'value': totals['cost_per_result'],
                'available': totals['cost_per_result'] is not None,
            }
        ],
    }


def _ab_interpretation(label, test):
    if not test.get('available'):
        return test.get('message', 'Amostra insuficiente.')
    if test.get('sample_warning'):
        return f'A amostra de {label} ainda é pequena; monitore antes de concluir.'
    if test.get('is_significant'):
        return f'A diferença de {label} parece estatisticamente relevante.'
    return f'Não há evidência estatística suficiente de diferença em {label}.'


def build_ab_tests(rows, selected_entity_ids, entity_type):
    selected = [str(value) for value in selected_entity_ids if str(value)]
    if len(selected) < 2:
        return {
            'available': False,
            'message': f'Selecione pelo menos duas entidades no nível {entity_type} para executar um teste A/B.',
            'comparisons': [],
        }

    grouped, names = _group_by_entity(rows)
    entity_a, entity_b = selected[:2]
    rows_a = grouped.get(entity_a, [])
    rows_b = grouped.get(entity_b, [])
    if not rows_a or not rows_b:
        return {
            'available': False,
            'message': 'As duas entidades selecionadas precisam ter dados no período.',
            'comparisons': [],
        }

    total_a = _aggregate(rows_a)
    total_b = _aggregate(rows_b)
    comparisons = []
    proportion_tests = [
        ('ctr', 'CTR', total_a['clicks'], total_a['impressions'], total_b['clicks'], total_b['impressions']),
        (
            'click_to_result',
            'taxa clique → resultado',
            total_a['results'],
            total_a['clicks'],
            total_b['results'],
            total_b['clicks'],
        ),
    ]
    for metric, label, success_a, base_a, success_b, base_b in proportion_tests:
        test = two_proportion_z_test(success_a, base_a, success_b, base_b)
        comparisons.append(
            {
                'test_type': 'two_proportion_z',
                'metric': metric,
                'metric_label': label,
                'entity_a': {'id': entity_a, 'name': names.get(entity_a, entity_a)},
                'entity_b': {'id': entity_b, 'name': names.get(entity_b, entity_b)},
                **test,
                'interpretation': _ab_interpretation(label, test),
            }
        )

    for metric in ('ctr', 'cpc', 'cost_per_result'):
        test = welch_mean_test(
            [row.get(metric) for row in rows_a],
            [row.get(metric) for row in rows_b],
        )
        comparisons.append(
            {
                'test_type': 'welch_mean',
                'metric': metric,
                'metric_label': f'{METRIC_CONFIG[metric]["label"]} diário',
                'entity_a': {'id': entity_a, 'name': names.get(entity_a, entity_a)},
                'entity_b': {'id': entity_b, 'name': names.get(entity_b, entity_b)},
                **test,
                'interpretation': _ab_interpretation(METRIC_CONFIG[metric]['label'], test),
            }
        )

    return {
        'available': any(item.get('available') for item in comparisons),
        'message': '',
        'comparisons': comparisons,
    }


def build_saturation(current_rows, previous_rows, entity_type):
    if not current_rows:
        return {
            'available': False,
            'message': 'Não há dados suficientes para avaliar saturação.',
            'items': [],
        }
    if not previous_rows:
        return {
            'available': False,
            'message': 'O período anterior não possui dados suficientes para avaliar saturação.',
            'items': [],
        }

    current_grouped, current_names = _group_by_entity(current_rows)
    previous_grouped, _ = _group_by_entity(previous_rows)
    items = []
    for entity_id, rows in current_grouped.items():
        current = _aggregate(rows)
        previous_entity_rows = previous_grouped.get(entity_id, [])
        previous = _aggregate(previous_entity_rows) if previous_entity_rows else {}
        changes = {
            metric: round_or_none(percent_change(current.get(metric), previous.get(metric)))
            for metric in ('frequency', 'ctr', 'cost_per_result', 'cpm', 'results', 'spend')
        }
        score = 0
        evidence = []
        if changes['frequency'] is not None and changes['frequency'] >= 20:
            score += 20
            evidence.append(f'Frequência +{changes["frequency"]:.1f}%')
        if changes['ctr'] is not None and changes['ctr'] <= -15:
            score += 25
            evidence.append(f'CTR {changes["ctr"]:.1f}%')
        if changes['cost_per_result'] is not None and changes['cost_per_result'] >= 20:
            score += 25
            evidence.append(f'Custo por resultado +{changes["cost_per_result"]:.1f}%')
        if changes['cpm'] is not None and changes['cpm'] >= 20:
            score += 15
            evidence.append(f'CPM +{changes["cpm"]:.1f}%')
        if changes['results'] is not None and changes['results'] <= -20:
            score += 15
            evidence.append(f'Resultados {changes["results"]:.1f}%')

        status = 'saturado' if score >= 65 else 'atenção' if score >= 30 else 'baixo risco'
        items.append(
            {
                'entity_type': entity_type,
                'entity_id': entity_id,
                'entity_name': current_names[entity_id],
                'frequency_current': current['frequency'],
                'frequency_previous': previous.get('frequency'),
                'changes': changes,
                'saturation_score': score,
                'status': status,
                'evidence': evidence,
                'interpretation': (
                    'Há sinais combinados de fadiga de público ou criativo.'
                    if score >= 65
                    else 'Existem sinais para monitorar e comparar com novos criativos.'
                    if score >= 30
                    else 'Não foram encontrados sinais fortes de saturação no período.'
                ),
            }
        )
    items.sort(key=lambda item: (-item['saturation_score'], item['entity_name']))
    return {
        'available': bool(items),
        'message': '' if items else 'Não há dados suficientes para avaliar saturação.',
        'items': items[:30],
    }


def build_trends(rows):
    daily_rows = _group_by_date(rows)
    metrics = []
    anomalies = []
    for metric in TREND_METRICS:
        values = [row.get(metric) for row in daily_rows]
        valid_values = [value for value in values if value is not None]
        if len(valid_values) < 2:
            metrics.append(
                {
                    'metric': metric,
                    'label': METRIC_CONFIG[metric]['label'],
                    'available': False,
                    'message': 'Amostra temporal insuficiente.',
                    'slope': None,
                    'trend': 'indisponível',
                    'points': [],
                }
            )
            continue

        normalized = [value if value is not None else 0.0 for value in values]
        rolling_7 = moving_average(normalized, 7)
        rolling_14 = moving_average(normalized, 14)
        scores = z_scores(normalized)
        slope = linear_trend_slope(normalized)
        points = []
        for index, row in enumerate(daily_rows):
            is_anomaly = len(normalized) >= 5 and scores[index] is not None and abs(scores[index]) >= 2
            point = {
                'date': row['date'],
                'value': round_or_none(values[index]),
                'rolling_average_7d': rolling_7[index],
                'rolling_average_14d': rolling_14[index],
                'z_score': scores[index],
                'is_anomaly': is_anomaly,
            }
            points.append(point)
            if is_anomaly:
                anomalies.append(
                    {
                        'date': row['date'],
                        'metric': metric,
                        'metric_label': METRIC_CONFIG[metric]['label'],
                        **point,
                        'interpretation': (
                            f'{METRIC_CONFIG[metric]["label"]} ficou fora do padrão recente.'
                        ),
                    }
                )
        trend = 'alta' if slope and slope > 0 else 'queda' if slope and slope < 0 else 'estável'
        metrics.append(
            {
                'metric': metric,
                'label': METRIC_CONFIG[metric]['label'],
                'available': True,
                'message': '',
                'slope': slope,
                'trend': trend,
                'points': points,
            }
        )
    anomalies.sort(key=lambda item: (item['date'], abs(item['z_score'] or 0)), reverse=True)
    return {
        'available': bool(daily_rows),
        'message': '' if daily_rows else 'Não há série diária no período selecionado.',
        'metrics': metrics,
        'anomalies': anomalies[:20],
    }


def build_correlations(rows):
    daily_rows = _group_by_date(rows)
    pairs = (
        ('frequency', 'ctr'),
        ('frequency', 'cost_per_result'),
        ('cpm', 'cpc'),
        ('ctr', 'cost_per_result'),
        ('spend', 'results'),
    )
    items = []
    for metric_x, metric_y in pairs:
        correlation = pearson_correlation(
            [row.get(metric_x) for row in daily_rows],
            [row.get(metric_y) for row in daily_rows],
        )
        if correlation is None:
            continue
        direction = 'positiva' if correlation > 0 else 'negativa' if correlation < 0 else 'neutra'
        items.append(
            {
                'metric_x': metric_x,
                'metric_x_label': METRIC_CONFIG[metric_x]['label'],
                'metric_y': metric_y,
                'metric_y_label': METRIC_CONFIG[metric_y]['label'],
                'correlation': correlation,
                'strength': correlation_strength(correlation),
                'direction': direction,
                'interpretation': (
                    f'{METRIC_CONFIG[metric_x]["label"]} e {METRIC_CONFIG[metric_y]["label"]} '
                    f'têm correlação {correlation_strength(correlation)} e {direction}.'
                ),
                'causality_warning': 'Correlação não implica causalidade.',
            }
        )
    return {
        'available': bool(items),
        'message': '' if items else 'Ainda não há dias suficientes ou variação para calcular correlações.',
        'items': items,
    }


def build_executive_insights(overview, stability, ab_tests, saturation, trends):
    insights = []
    efficient_candidates = [
        item
        for item in stability.get('items', [])
        if item['metric'] == 'cost_per_result' and item['mean'] is not None and item['sample_size'] >= 2
    ]
    if efficient_candidates:
        efficient = min(efficient_candidates, key=lambda item: item['mean'])
        insights.append(
            {
                'type': 'success',
                'title': f'Maior eficiência em {efficient["entity_name"]}',
                'description': (
                    f'{efficient["entity_name"]} apresentou o menor custo médio por resultado entre as entidades analisadas.'
                ),
                'evidence': [f'Custo médio {efficient["mean"]:.2f}', efficient['stability_label']],
                'suggested_action': 'Comparar qualidade e volume antes de ampliar investimento.',
            }
        )

    saturation_items = saturation.get('items', [])
    if saturation_items and saturation_items[0]['saturation_score'] >= 30:
        item = saturation_items[0]
        insights.append(
            {
                'type': 'danger' if item['status'] == 'saturado' else 'warning',
                'title': f'Possível saturação em {item["entity_name"]}',
                'description': item['interpretation'],
                'evidence': item['evidence'],
                'suggested_action': 'Avaliar troca de criativo, expansão de público ou redistribuição gradual.',
            }
        )

    unstable = next(
        (item for item in stability.get('items', []) if item['stability_label'] == 'instável'),
        None,
    )
    if unstable:
        insights.append(
            {
                'type': 'warning',
                'title': f'{unstable["entity_name"]} apresenta instabilidade',
                'description': unstable['interpretation'],
                'evidence': [f'CV {unstable["coefficient_of_variation"]:.2f}', unstable['metric_label']],
                'suggested_action': 'Investigar os dias de maior variação antes de alterar orçamento.',
            }
        )

    significant = next(
        (item for item in ab_tests.get('comparisons', []) if item.get('is_significant')),
        None,
    )
    if significant:
        insights.append(
            {
                'type': 'success',
                'title': f'Diferença relevante em {significant["metric_label"]}',
                'description': significant['interpretation'],
                'evidence': [f'p-value {significant["p_value"]:.4f}'],
                'suggested_action': 'Comparar também impacto prático e qualidade antes de escalar.',
            }
        )

    anomaly = next(iter(trends.get('anomalies', [])), None)
    if anomaly:
        insights.append(
            {
                'type': 'warning',
                'title': f'Anomalia recente em {anomaly["metric_label"]}',
                'description': anomaly['interpretation'],
                'evidence': [anomaly['date'], f'z-score {anomaly["z_score"]:.2f}'],
                'suggested_action': 'Revisar alterações de orçamento, público, criativo e tracking na data.',
            }
        )

    positive_changes = [
        metric
        for metric in overview.get('metrics', [])
        if metric['direction'] == 'positive' and metric['percent_change'] is not None
    ]
    if positive_changes:
        best = max(positive_changes, key=lambda metric: abs(metric['percent_change']))
        insights.append(
            {
                'type': 'success',
                'title': f'Melhor oportunidade: {best["label"]}',
                'description': best['interpretation'],
                'evidence': [f'{best["percent_change"]:+.2f}% contra o período anterior'],
                'suggested_action': 'Monitorar a consistência antes de ampliar a estratégia.',
            }
        )

    if not insights:
        insights.append(
            {
                'type': 'info',
                'title': 'Sem alertas estatísticos fortes',
                'description': 'A amostra atual não apresentou evidências suficientes para um alerta executivo.',
                'evidence': [],
                'suggested_action': 'Monitorar mais dias ou refinar a seleção de entidades.',
            }
        )
    return {'available': True, 'message': '', 'items': insights[:8]}


def build_statistics_analysis(
    *,
    current_rows,
    previous_rows,
    compare,
    entity_type,
    selected_entity_ids,
):
    current = _normalize_rows(current_rows)
    previous = _normalize_rows(previous_rows)
    overview = build_overview(current, previous, compare)
    stability = build_stability(current, entity_type)
    funnel = build_funnel(current)
    ab_tests = build_ab_tests(current, selected_entity_ids, entity_type)
    saturation = build_saturation(current, previous if compare else [], entity_type)
    trends = build_trends(current)
    correlations = build_correlations(current)
    unavailable_segments = {
        'available': False,
        'message': 'Breakdowns de idade, gênero, plataforma e posicionamento não estão persistidos no banco atual.',
        'breakdown': None,
        'items': [],
    }
    unavailable_cohorts = {
        'available': False,
        'message': 'Dados comerciais insuficientes para análise de coorte completa.',
        'items': [],
    }
    return {
        'overview': overview,
        'stability': stability,
        'funnel': funnel,
        'segments': unavailable_segments,
        'ab_tests': ab_tests,
        'saturation': saturation,
        'cohorts': unavailable_cohorts,
        'trends': trends,
        'correlations': correlations,
        'executive_insights': build_executive_insights(
            overview,
            stability,
            ab_tests,
            saturation,
            trends,
        ),
    }
