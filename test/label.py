__author__ = 'alse'


def jaccard_similarity(x, y):
    if not x and not y:
        return 0
    intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
    union_cardinality = len(set.union(*[set(x), set(y)]))
    return intersection_cardinality / float(union_cardinality)


def get_n_grams(sentence, n):
    return [sentence[i:i + n] for i in xrange(len(sentence) - n)]


def label_text_test(train_label, test_label, num1, num2):
    return jaccard_similarity(get_n_grams(train_label, 2), get_n_grams(test_label, 2))
