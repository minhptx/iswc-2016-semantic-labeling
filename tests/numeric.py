from numpy import percentile
from scipy.stats import mannwhitneyu, f_oneway, ks_2samp, ttest_ind

from tests import balance_result

__author__ = 'alse'


def kolmogorov_smirnov_test(train_examples, test_examples, num1, num2):
    if len(train_examples) > 1 and len(test_examples) > 1:
        result = ks_2samp(train_examples, test_examples)[1]
        return balance_result(num1, num2, True, result)
    return 0


def welch_test(train_examples, test_examples, num1, num2):
    if len(train_examples) > 1 and len(test_examples) > 1:
        result = ttest_ind(train_examples, test_examples, False)[1]
        return balance_result(num1, num2, True, result)
    return 0


def mann_whitney_test(train_examples, test_examples, num1, num2):
    if len(train_examples) > 1 and len(test_examples) > 1:
        if test_examples[-1] != 0 and train_examples[-1] != 0:
            result = mannwhitneyu(train_examples, test_examples)[1]
            return result
    return 0


def mann_whitney_u_test(train_examples, test_examples, num1, num2):
    if len(train_examples) > 1 and len(test_examples) > 1:
        result = mannwhitneyu(train_examples, test_examples)[1]
        return balance_result(num1, num2, True, result)
    return 0


def anova_test(train_examples, test_examples):
    if test_examples[-1] > 50 or train_examples[-1] > 50:
        return 0
    if len(train_examples) > 1 and len(test_examples) > 1:
        result = f_oneway(train_examples, test_examples).pvalue
        return result
    return 0


def coverage_test(train_examples, test_examples, num1, num2):
    if len(train_examples) > 1 and len(test_examples) > 1:
        max1 = percentile(train_examples, 75)
        min1 = percentile(train_examples, 25)
        max2 = percentile(test_examples, 75)
        min2 = percentile(test_examples, 25)
        max3 = max(max1, max2)
        min3 = min(min1, min2)
        if min2 > max1 or min1 > max2:
            return 0
        elif max3 == min3:
            return 0
        else:
            min4 = min(max1, max2)
            max4 = max(min1, min2)
            result = (min4 - max4) * 1.0 / (max3 - min3)
            return balance_result(num1, num2, True, result)
    return 0
