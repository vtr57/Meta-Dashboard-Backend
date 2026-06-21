from __future__ import annotations

from collections import defaultdict
from statistics import median

from Dashboard.services.statistics_utils import (
    deterministic_kmeans,
    pca_projection,
    round_or_none,
    safe_ratio,
    standardize_matrix,
)


FEATURE_CONFIG = {
    'spend': {'label': 'Investimento', 'format': 'currency'},
    'impressions': {'label': 'Impressões', 'format': 'number'},
    'clicks': {'label': 'Cliques', 'format': 'number'},
    'ctr': {'label': 'CTR', 'format': 'percent'},
    'cpc': {'label': 'CPC', 'format': 'currency'},
    'cpm': {'label': 'CPM', 'format': 'currency'},
    'results': {'label': 'Resultados', 'format': 'number'},
    'cost_per_result': {'label': 'Custo por resultado', 'format': 'currency'},
    'conversion_rate': {'label': 'Taxa clique → resultado', 'format': 'percent'},
    'frequency': {'label': 'Frequência', 'format': 'decimal'},
}

DEFAULT_FEATURES = {
    'campaign': tuple(FEATURE_CONFIG),
    'adset': tuple(FEATURE_CONFIG),
    'ad': tuple(FEATURE_CONFIG),
}

ENTITY_LABELS = {
    'campaign': ('campanha', 'campanhas'),
    'adset': ('conjunto', 'conjuntos'),
    'ad': ('anúncio', 'anúncios'),
}


def _aggregate_entities(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[str(row.get('entity_id') or '')].append(row)

    entities = []
    for entity_id, entity_rows in grouped.items():
        spend = sum(float(row.get('spend') or 0) for row in entity_rows)
        impressions = sum(int(row.get('impressions') or 0) for row in entity_rows)
        reach = sum(int(row.get('reach') or 0) for row in entity_rows)
        clicks = sum(int(row.get('clicks') or 0) for row in entity_rows)
        results = sum(int(row.get('results') or 0) for row in entity_rows)
        activity = spend + impressions + reach + clicks + results
        if activity <= 0:
            continue
        entities.append(
            {
                'id': entity_id,
                'name': entity_rows[0].get('entity_name') or entity_id,
                'spend': round_or_none(spend),
                'impressions': impressions,
                'reach': reach,
                'clicks': clicks,
                'ctr': round_or_none(safe_ratio(clicks, impressions, 100)),
                'cpc': round_or_none(safe_ratio(spend, clicks)),
                'cpm': round_or_none(safe_ratio(spend, impressions, 1000)),
                'results': results,
                'cost_per_result': round_or_none(safe_ratio(spend, results)),
                'conversion_rate': round_or_none(safe_ratio(results, clicks, 100)),
                'frequency': round_or_none(safe_ratio(impressions, reach)),
            }
        )
    return entities


def _prepare_features(entities, requested_features):
    warnings = []
    features_used = []
    feature_medians = {}

    for feature in requested_features:
        available_values = [
            float(entity[feature])
            for entity in entities
            if entity.get(feature) is not None
        ]
        distinct_values = {round(value, 10) for value in available_values}
        if len(distinct_values) < 2:
            warnings.append(
                f'A métrica {FEATURE_CONFIG[feature]["label"]} foi ignorada por não apresentar variação suficiente.'
            )
            continue
        features_used.append(feature)
        feature_medians[feature] = float(median(available_values))

    if len(features_used) < 2:
        return {
            'features_used': features_used,
            'matrix': [],
            'warnings': warnings,
            'imputed_features': [],
        }

    imputed_features = set()
    matrix = []
    for entity in entities:
        feature_row = []
        for feature in features_used:
            value = entity.get(feature)
            if value is None:
                value = feature_medians[feature]
                imputed_features.add(feature)
            feature_row.append(float(value))
        matrix.append(feature_row)

    if imputed_features:
        labels = ', '.join(FEATURE_CONFIG[feature]['label'] for feature in sorted(imputed_features))
        warnings.append(
            f'Valores indisponíveis em {labels} foram preenchidos pela mediana da amostra.'
        )

    return {
        'features_used': features_used,
        'matrix': matrix,
        'warnings': warnings,
        'imputed_features': sorted(imputed_features),
    }


def _allowed_clusters(sample_size):
    if sample_size < 5:
        return 0
    if sample_size <= 10:
        return 2
    if sample_size <= 30:
        return 3
    return 5


def _average(items, metric):
    values = [float(item[metric]) for item in items if item.get(metric) is not None]
    return round_or_none(sum(values) / len(values)) if values else None


def _relative(value, overall, threshold=0.12):
    if value is None or overall is None:
        return 'unknown'
    if overall == 0:
        if value == 0:
            return 'mid'
        return 'high'
    ratio = (value - overall) / abs(overall)
    if ratio >= threshold:
        return 'high'
    if ratio <= -threshold:
        return 'low'
    return 'mid'


def _cluster_label(summary, overall, entity_type):
    spend = _relative(summary.get('avg_spend'), overall.get('spend'))
    results = _relative(summary.get('avg_results'), overall.get('results'))
    ctr = _relative(summary.get('avg_ctr'), overall.get('ctr'))
    cost = _relative(summary.get('avg_cost_per_result'), overall.get('cost_per_result'))
    conversion = _relative(summary.get('avg_conversion_rate'), overall.get('conversion_rate'))
    frequency = _relative(summary.get('avg_frequency'), overall.get('frequency'))

    if spend == 'high' and results == 'low' and cost == 'high':
        return 'Alto gasto e baixo retorno'
    if frequency == 'high' and ctr == 'low' and cost == 'high':
        return 'Possível saturação'
    if ctr == 'high' and conversion == 'low':
        return 'Alto interesse e baixa conversão'
    if results == 'high' and cost == 'low':
        return 'Alto volume e boa eficiência'
    if results == 'high' and cost == 'high':
        return 'Alto volume e baixa eficiência'
    if spend == 'low' and ctr == 'high' and cost == 'low':
        return 'Criativos promissores' if entity_type == 'ad' else 'Grupo promissor'
    if results == 'low' and spend in {'low', 'mid'}:
        return 'Baixo volume e baixa entrega'
    if cost == 'low' and conversion == 'high':
        return 'Baixo volume e alta qualidade' if results != 'high' else 'Alta eficiência'
    return 'Perfil equilibrado'


def _cluster_copy(label, entity_type):
    singular, plural = ENTITY_LABELS[entity_type]
    copies = {
        'Alto gasto e baixo retorno': (
            f'Grupo de {plural} com investimento acima da média e retorno relativo baixo.',
            'Revisar oferta, segmentação e criativos antes de ampliar orçamento.',
        ),
        'Possível saturação': (
            f'Grupo de {plural} com frequência alta e sinais combinados de perda de eficiência.',
            'Avaliar renovação de criativos e expansão de público.',
        ),
        'Alto interesse e baixa conversão': (
            f'Os {plural} atraem cliques, mas transformam menos cliques em resultados.',
            'Revisar promessa, página de destino e continuidade da jornada.',
        ),
        'Alto volume e boa eficiência': (
            f'Grupo de {plural} com volume acima da média e custo por resultado mais eficiente.',
            'Avaliar aumento gradual de orçamento, preservando qualidade e estabilidade.',
        ),
        'Alto volume e baixa eficiência': (
            f'Grupo de {plural} com volume relevante, porém custo por resultado acima da média.',
            'Investigar fontes de desperdício antes de buscar mais escala.',
        ),
        'Criativos promissores': (
            'Anúncios com bons sinais de interesse e eficiência relativa.',
            'Testar escala gradual e novas variações do mesmo conceito criativo.',
        ),
        'Grupo promissor': (
            f'Grupo de {plural} com sinais positivos e investimento ainda abaixo da média.',
            'Avaliar escala gradual com monitoramento de custo e qualidade.',
        ),
        'Baixo volume e baixa entrega': (
            f'Grupo de {plural} com pouca entrega e poucos resultados no período.',
            f'Validar elegibilidade, orçamento, público e relevância de cada {singular}.',
        ),
        'Baixo volume e alta qualidade': (
            f'Grupo de {plural} pequeno, mas com boa eficiência relativa.',
            'Aumentar exposição com cautela para confirmar se a eficiência se sustenta.',
        ),
        'Alta eficiência': (
            f'Grupo de {plural} com custo e conversão favoráveis em relação à amostra.',
            'Preservar a configuração e testar expansão gradual.',
        ),
        'Perfil equilibrado': (
            f'Grupo de {plural} próximo das médias gerais, sem desvio dominante.',
            'Monitorar e buscar testes incrementais antes de mudanças maiores.',
        ),
    }
    return copies[label]


def _risk_score(summary, overall):
    score = 0
    if _relative(summary.get('avg_spend'), overall.get('spend')) == 'high':
        score += 1
    if _relative(summary.get('avg_cost_per_result'), overall.get('cost_per_result')) == 'high':
        score += 2
    if _relative(summary.get('avg_ctr'), overall.get('ctr')) == 'low':
        score += 1
    if _relative(summary.get('avg_conversion_rate'), overall.get('conversion_rate')) == 'low':
        score += 1
    if _relative(summary.get('avg_frequency'), overall.get('frequency')) == 'high':
        score += 1
    return score


def _efficiency_score(summary, overall):
    score = 0
    if _relative(summary.get('avg_cost_per_result'), overall.get('cost_per_result')) == 'low':
        score += 2
    if _relative(summary.get('avg_cpc'), overall.get('cpc')) == 'low':
        score += 1
    if _relative(summary.get('avg_ctr'), overall.get('ctr')) == 'high':
        score += 1
    if _relative(summary.get('avg_conversion_rate'), overall.get('conversion_rate')) == 'high':
        score += 1
    if _relative(summary.get('avg_results'), overall.get('results')) == 'high':
        score += 1
    return score


def _build_insights(clusters, sample_size, entity_type):
    if not clusters:
        return {'available': False, 'message': 'Não há clusters disponíveis.', 'items': []}

    _, plural = ENTITY_LABELS[entity_type]
    risky = max(clusters, key=lambda item: (item['risk_score'], item['size']))
    efficient = max(clusters, key=lambda item: (item['efficiency_score'], item['size']))
    insights = []

    if risky['risk_score'] >= 2:
        insights.append(
            {
                'type': 'warning',
                'title': risky['label'],
                'description': f'{risky["size"]} {plural} foram agrupados com sinais relativos de risco.',
                'evidence': risky['evidence'],
                'suggested_action': risky['suggested_action'],
            }
        )
    if efficient['efficiency_score'] >= 2 and efficient['cluster_id'] != risky['cluster_id']:
        insights.append(
            {
                'type': 'success',
                'title': f'Oportunidade em {efficient["label"].lower()}',
                'description': f'{efficient["size"]} {plural} formam o grupo mais eficiente da amostra.',
                'evidence': efficient['evidence'],
                'suggested_action': efficient['suggested_action'],
            }
        )

    if not insights:
        insights.append(
            {
                'type': 'info',
                'title': 'Clusters sem contraste forte',
                'description': 'Os grupos encontrados estão relativamente próximos das médias gerais.',
                'evidence': [f'{sample_size} {plural} analisados'],
                'suggested_action': 'Amplie o período ou refine os filtros para buscar padrões mais distintos.',
            }
        )
    return {'available': True, 'message': '', 'items': insights}


def build_clustering_analysis(
    *,
    rows,
    entity_type,
    requested_clusters,
    normalize=True,
):
    capabilities = {
        'entity_types': {
            'campaign': True,
            'adset': True,
            'ad': True,
            'lead': False,
        },
        'algorithms': {
            'kmeans': True,
            'dbscan': False,
            'hierarchical': False,
        },
        'pca': True,
    }
    if entity_type == 'lead':
        return {
            'available': False,
            'message': 'Não há dados comerciais suficientes para clusterizar leads.',
            'sample_size': 0,
            'clusters_count': 0,
            'features_used': [],
            'warnings': [],
            'summary': {},
            'clusters': [],
            'items': [],
            'pca': {'available': False, 'message': 'PCA indisponível sem dados.', 'points': []},
            'executive_insights': {'available': False, 'message': 'Sem dados de leads.', 'items': []},
            'capabilities': capabilities,
        }

    entities = _aggregate_entities(rows)
    sample_size = len(entities)
    maximum_clusters = _allowed_clusters(sample_size)
    if maximum_clusters == 0:
        return {
            'available': False,
            'message': (
                'A clusterização precisa de pelo menos 5 campanhas, conjuntos ou anúncios '
                'com dados válidos. Selecione um período maior ou reduza os filtros.'
            ),
            'sample_size': sample_size,
            'clusters_count': 0,
            'features_used': [],
            'warnings': [],
            'summary': {},
            'clusters': [],
            'items': [],
            'pca': {'available': False, 'message': 'Amostra insuficiente para PCA.', 'points': []},
            'executive_insights': {'available': False, 'message': 'Amostra insuficiente.', 'items': []},
            'capabilities': capabilities,
        }

    warnings = []
    clusters_count = min(requested_clusters, maximum_clusters, sample_size)
    if clusters_count != requested_clusters:
        warnings.append(
            f'A quantidade de clusters foi reduzida para {clusters_count} porque a amostra possui '
            f'apenas {sample_size} entidades.'
        )
    if sample_size <= 10:
        warnings.append(
            'A amostra é pequena; use os grupos como sinal exploratório, não como conclusão definitiva.'
        )

    feature_data = _prepare_features(entities, DEFAULT_FEATURES[entity_type])
    warnings.extend(feature_data['warnings'])
    features_used = feature_data['features_used']
    if len(features_used) < 2:
        return {
            'available': False,
            'message': 'Não há variação suficiente nas métricas para formar clusters confiáveis.',
            'sample_size': sample_size,
            'clusters_count': 0,
            'features_used': features_used,
            'warnings': warnings,
            'summary': {},
            'clusters': [],
            'items': [],
            'pca': {'available': False, 'message': 'Features insuficientes para PCA.', 'points': []},
            'executive_insights': {'available': False, 'message': 'Features insuficientes.', 'items': []},
            'capabilities': capabilities,
        }

    standardized = standardize_matrix(feature_data['matrix'], enabled=normalize)
    clustering = deterministic_kmeans(standardized['matrix'], clusters_count)
    pca = pca_projection(standardized['matrix'])

    for index, entity in enumerate(entities):
        entity['cluster_id'] = clustering['labels'][index]
        entity['cluster_distance'] = clustering['distances'][index]

    overall = {
        feature: _average(entities, feature)
        for feature in FEATURE_CONFIG
    }
    clusters = []
    for cluster_id in range(clusters_count):
        cluster_items = [entity for entity in entities if entity['cluster_id'] == cluster_id]
        summary = {
            f'avg_{feature}': _average(cluster_items, feature)
            for feature in FEATURE_CONFIG
        }
        label = _cluster_label(summary, overall, entity_type)
        interpretation, suggested_action = _cluster_copy(label, entity_type)
        evidence = [
            f'CTR médio: {summary["avg_ctr"]:.2f}%' if summary['avg_ctr'] is not None else 'CTR indisponível',
            (
                f'Custo por resultado médio: {summary["avg_cost_per_result"]:.2f}'
                if summary['avg_cost_per_result'] is not None
                else 'Custo por resultado indisponível'
            ),
            f'Resultados médios: {summary["avg_results"]:.2f}' if summary['avg_results'] is not None else '',
        ]
        evidence = [item for item in evidence if item]
        clusters.append(
            {
                'cluster_id': cluster_id,
                'label': label,
                'size': len(cluster_items),
                'summary': summary,
                'interpretation': interpretation,
                'suggested_action': suggested_action,
                'risk_score': _risk_score(summary, overall),
                'efficiency_score': _efficiency_score(summary, overall),
                'evidence': evidence,
                'items': sorted(cluster_items, key=lambda item: item['cluster_distance']),
            }
        )

    efficient = max(clusters, key=lambda item: (item['efficiency_score'], item['size']))
    risky = max(clusters, key=lambda item: (item['risk_score'], item['size']))
    pca_points = []
    if pca['available']:
        pca_points = [
            {
                'id': entity['id'],
                'name': entity['name'],
                'x': pca['points'][index][0],
                'y': pca['points'][index][1],
                'cluster_id': entity['cluster_id'],
            }
            for index, entity in enumerate(entities)
        ]

    return {
        'available': True,
        'message': '',
        'sample_size': sample_size,
        'clusters_count': clusters_count,
        'features_used': [
            {
                'key': feature,
                **FEATURE_CONFIG[feature],
            }
            for feature in features_used
        ],
        'warnings': warnings,
        'summary': {
            'total_entities': sample_size,
            'clusters_count': clusters_count,
            'most_efficient_cluster_id': efficient['cluster_id'],
            'most_efficient_cluster_label': efficient['label'],
            'highest_risk_cluster_id': risky['cluster_id'],
            'highest_risk_cluster_label': risky['label'],
            'outliers_count': 0,
        },
        'clusters': clusters,
        'items': sorted(entities, key=lambda item: (item['cluster_id'], item['cluster_distance'])),
        'pca': {
            **pca,
            'points': pca_points,
        },
        'executive_insights': _build_insights(clusters, sample_size, entity_type),
        'capabilities': capabilities,
        'normalization': {
            'enabled': normalize,
            'method': 'z-score' if normalize else 'none',
            'means': standardized['means'],
            'standard_deviations': standardized['standard_deviations'],
        },
        'iterations': clustering['iterations'],
    }
