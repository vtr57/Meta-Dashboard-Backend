from __future__ import annotations

import math
from statistics import NormalDist, mean, median, pstdev

from scipy.stats import ttest_ind


def to_number(value, default=0.0) -> float:
    if value in (None, ''):
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def safe_ratio(numerator, denominator, multiplier=1.0, default=None):
    denominator_value = to_number(denominator)
    if denominator_value == 0:
        return default
    return (to_number(numerator) / denominator_value) * multiplier


def percent_change(current, previous):
    if current is None or previous is None:
        return None
    current_value = to_number(current)
    previous_value = to_number(previous)
    if previous_value == 0:
        return 0.0 if current_value == 0 else None
    return ((current_value - previous_value) / previous_value) * 100.0


def round_or_none(value, digits=4):
    if value is None:
        return None
    value = to_number(value)
    if not math.isfinite(value):
        return None
    return round(value, digits)


def stability_label(coefficient_of_variation):
    if coefficient_of_variation is None:
        return 'amostra insuficiente'
    if coefficient_of_variation <= 0.25:
        return 'estável'
    if coefficient_of_variation <= 0.60:
        return 'moderadamente instável'
    return 'instável'


def descriptive_statistics(values):
    normalized = [to_number(value) for value in values if value is not None and math.isfinite(to_number(value))]
    if not normalized:
        return {
            'sample_size': 0,
            'mean': None,
            'median': None,
            'std_dev': None,
            'coefficient_of_variation': None,
            'minimum': None,
            'maximum': None,
            'range': None,
            'stability_label': 'amostra insuficiente',
        }

    average = mean(normalized)
    std_dev = pstdev(normalized) if len(normalized) > 1 else 0.0
    coefficient = abs(std_dev / average) if average != 0 else None
    minimum = min(normalized)
    maximum = max(normalized)
    return {
        'sample_size': len(normalized),
        'mean': round_or_none(average),
        'median': round_or_none(median(normalized)),
        'std_dev': round_or_none(std_dev),
        'coefficient_of_variation': round_or_none(coefficient),
        'minimum': round_or_none(minimum),
        'maximum': round_or_none(maximum),
        'range': round_or_none(maximum - minimum),
        'stability_label': stability_label(coefficient),
    }


def two_proportion_z_test(success_a, total_a, success_b, total_b, confidence_level=95):
    success_a = to_number(success_a)
    success_b = to_number(success_b)
    total_a = to_number(total_a)
    total_b = to_number(total_b)
    if total_a <= 0 or total_b <= 0:
        return {
            'available': False,
            'message': 'Amostra insuficiente para o teste de proporção.',
        }
    if success_a < 0 or success_b < 0 or success_a > total_a or success_b > total_b:
        return {
            'available': False,
            'message': 'A métrica não forma uma proporção válida para esta amostra.',
        }

    rate_a = success_a / total_a
    rate_b = success_b / total_b
    pooled = (success_a + success_b) / (total_a + total_b)
    standard_error = math.sqrt(max(pooled * (1 - pooled) * ((1 / total_a) + (1 / total_b)), 0))
    if standard_error == 0:
        p_value = 1.0 if rate_a == rate_b else 0.0
        z_score = 0.0 if rate_a == rate_b else math.inf
    else:
        z_score = (rate_a - rate_b) / standard_error
        p_value = 2 * (1 - NormalDist().cdf(abs(z_score)))

    alpha = 1 - (confidence_level / 100)
    sample_warning = min(total_a, total_b) < 100 or min(success_a, success_b) < 5
    return {
        'available': True,
        'rate_a': round_or_none(rate_a),
        'rate_b': round_or_none(rate_b),
        'absolute_difference': round_or_none(rate_a - rate_b),
        'percent_difference': round_or_none(percent_change(rate_a, rate_b)),
        'z_score': round_or_none(z_score),
        'p_value': round_or_none(p_value, 6),
        'is_significant': bool(p_value < alpha and not sample_warning),
        'confidence_level': confidence_level,
        'sample_warning': sample_warning,
    }


def welch_mean_test(values_a, values_b, confidence_level=95):
    sample_a = [to_number(value) for value in values_a if value is not None]
    sample_b = [to_number(value) for value in values_b if value is not None]
    if len(sample_a) < 2 or len(sample_b) < 2:
        return {
            'available': False,
            'message': 'São necessários pelo menos dois dias válidos por entidade.',
        }

    statistic, p_value = ttest_ind(sample_a, sample_b, equal_var=False, nan_policy='omit')
    if not math.isfinite(float(p_value)):
        return {
            'available': False,
            'message': 'Não houve variação suficiente para comparar as médias.',
        }

    alpha = 1 - (confidence_level / 100)
    sample_warning = min(len(sample_a), len(sample_b)) < 7
    mean_a = mean(sample_a)
    mean_b = mean(sample_b)
    return {
        'available': True,
        'mean_a': round_or_none(mean_a),
        'mean_b': round_or_none(mean_b),
        'absolute_difference': round_or_none(mean_a - mean_b),
        'percent_difference': round_or_none(percent_change(mean_a, mean_b)),
        'test_statistic': round_or_none(statistic),
        'p_value': round_or_none(p_value, 6),
        'is_significant': bool(p_value < alpha and not sample_warning),
        'confidence_level': confidence_level,
        'sample_warning': sample_warning,
    }


def moving_average(values, window):
    averages = []
    for index in range(len(values)):
        start = max(0, index - window + 1)
        sample = [to_number(value) for value in values[start : index + 1] if value is not None]
        averages.append(round_or_none(mean(sample)) if sample else None)
    return averages


def linear_trend_slope(values):
    normalized = [to_number(value) for value in values]
    size = len(normalized)
    if size < 2:
        return None
    x_mean = (size - 1) / 2
    y_mean = mean(normalized)
    denominator = sum((index - x_mean) ** 2 for index in range(size))
    if denominator == 0:
        return None
    numerator = sum((index - x_mean) * (value - y_mean) for index, value in enumerate(normalized))
    return round_or_none(numerator / denominator)


def z_scores(values):
    normalized = [to_number(value) for value in values]
    if len(normalized) < 2:
        return [None for _ in normalized]
    standard_deviation = pstdev(normalized)
    if standard_deviation == 0:
        return [0.0 for _ in normalized]
    average = mean(normalized)
    return [round_or_none((value - average) / standard_deviation) for value in normalized]


def pearson_correlation(values_x, values_y):
    pairs = [
        (to_number(value_x), to_number(value_y))
        for value_x, value_y in zip(values_x, values_y)
        if value_x is not None and value_y is not None
    ]
    if len(pairs) < 3:
        return None
    x_values = [pair[0] for pair in pairs]
    y_values = [pair[1] for pair in pairs]
    x_mean = mean(x_values)
    y_mean = mean(y_values)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in pairs)
    x_denominator = math.sqrt(sum((x - x_mean) ** 2 for x in x_values))
    y_denominator = math.sqrt(sum((y - y_mean) ** 2 for y in y_values))
    if x_denominator == 0 or y_denominator == 0:
        return None
    return round_or_none(numerator / (x_denominator * y_denominator))


def correlation_strength(value):
    if value is None:
        return 'indisponível'
    absolute = abs(to_number(value))
    if absolute < 0.20:
        return 'muito fraca'
    if absolute < 0.40:
        return 'fraca'
    if absolute < 0.60:
        return 'moderada'
    if absolute < 0.80:
        return 'forte'
    return 'muito forte'
