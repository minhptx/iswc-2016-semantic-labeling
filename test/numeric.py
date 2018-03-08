import logging

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
        print(train_examples, test_examples)
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
    try:
        if len(train_examples) > 1 and len(test_examples) > 1:
            result = mannwhitneyu(train_examples, test_examples)[1]
            return balance_result(num1, num2, True, result)
        return 0
    except ValueError as e:
        logging.warn("IGNORE EXCEPTION: %s", str(e))
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
        max1 = percentile(train_examples, 100)
        min1 = percentile(train_examples, 0)
        max2 = percentile(test_examples, 100)
        min2 = percentile(test_examples, 0)
        max3 = max(max1, max2)
        min3 = min(min1, min2)
        # print "max1", max1
        # print "min1", min1
        # print "max2", max2
        # print "min2", min2
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
