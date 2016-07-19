from collections import defaultdict

from lib.utils import is_column_based, is_tree_based
from main import sc
from numeric import *
from tests.label import label_text_test
from tests.textual import *

__author__ = 'alse'

IS_NUMERIC = "IS_NUM"
KS_TEST = "KS"
MW_NUM_TEST = "MW_NUM"
MW_TEST = "MW"
ANOVA_TEST = "ANOVA"
ABBR_TEST = "ABBR"
LBL_ABBR_TEST = "LBL ABBR"
LBL_TEST = "LBL"
STR_LEN_TEST = "SIZE"
CHAR_LEN_TEST = "CHAR_SIZE"
TF_IDF_TEST = "TF_IDF"
AVG_TEST = "AVG"
WORD2VEC_TEST = "WORD2VEC"
COVER_TEST = "COVERAGE"
W_TEST = "WELCH"
JACCARD_TEST = "JACCARD"

feature_tests_map = {ANOVA_TEST: anova_test, KS_TEST: kolmogorov_smirnov_test, JACCARD_TEST: jaccard_test,
                     LBL_TEST: label_text_test, COVER_TEST: coverage_test, ABBR_TEST: abbr_test, W_TEST: welch_test,
                     MW_NUM_TEST: mann_whitney_u_test, WORD2VEC_TEST: word2vec_cosine_test, MW_TEST: mann_whitney_test,
                     CHAR_LEN_TEST: char_len_test, STR_LEN_TEST: len_test}

data_tests_map = {"textual_list": [JACCARD_TEST], "values": [ABBR_TEST], 'word2vec': [WORD2VEC_TEST],
                  'numeric_list': [KS_TEST, COVER_TEST, W_TEST], "char_lengths": [CHAR_LEN_TEST],
                  "word_lengths": [STR_LEN_TEST], "name": [LBL_TEST], "histogram": [MW_TEST]}

feature_list = [LBL_TEST, COVER_TEST, JACCARD_TEST, TF_IDF_TEST, KS_TEST, MW_TEST]
text_list = [ABBR_TEST, JACCARD_TEST, TF_IDF_TEST]
number_list = [COVER_TEST, KS_TEST, W_TEST]

tree_feature_list = []
for feature in feature_list:
    for i in range(5):
        tree_feature_list.append(feature + str(i))


def get_test_results(train_examples_map, textual_train_map, test_examples_map, is_labeled=False):
    def zip_with_key(key, item_map):
        result_list = []
        for value in item_map.items():
            if value[0] not in data_tests_map:
                continue
            for test_name in data_tests_map[value[0]]:
                if test_name in feature_list:
                    row = {'name': key, 'data_type': value[0], 'test_name': test_name, 'values': value[1],
                           'num': item_map['is_numeric']}
                    result_list.append(row)
        return result_list

    feature_vectors = defaultdict(lambda: defaultdict(lambda: 0))

    if is_column_based:
        train_data_rdd = sc.parallelize(train_examples_map).map(lambda hit: hit['_source']).flatMap(
            lambda hit: zip_with_key("%s" % (hit['semantic_type']), hit))

        test_results = train_data_rdd.map(lambda row: ((row['name'], row['test_name']), row)).mapValues(
            lambda row: round(
                feature_tests_map[row['test_name']](row['values'], test_examples_map[row['data_type']], row['num'],
                                                    test_examples_map['is_numeric']), 2)).reduceByKey(max).collect()
    else:
        train_data_rdd = sc.parallelize(train_examples_map).map(
            lambda hit: (hit['_source']['semantic_type'], hit['_source'])).flatMap(
            lambda row: [((row[0], x), row[1][x]) for x in
                         ['char_length', 'histogram', "numeric"
                             , 'values']]).reduceByKey(
            lambda val1, val2: val1 + val2 if isinstance(val1, list) else val1 + " " + val2)

        test_results = train_data_rdd.flatMap(
            lambda row: [((row[0][0], x), round(feature_tests_map[x](row[1], test_examples_map[row[0][1]]), 2)) for
                         x in data_tests_map[row[0][1]]]).collect()

    for result in sorted(test_results):
        feature_vectors[result[0][0]][result[0][1]] = result[1]

    for hit in textual_train_map['hits']['hits']:
        source = hit['_source']
        score = hit['_score']
        if is_column_based:
            name = "%s" % (source['semantic_type'])
        else:
            name = "%s" % (source['semantic_type'])
        score = balance_result(source["is_numeric"], test_examples_map["is_numeric"], False, score)
        if feature_vectors[name][TF_IDF_TEST] < score:
            feature_vectors[name][TF_IDF_TEST] = score

    if is_tree_based:
        for name in feature_vectors.keys():
            for test in feature_vectors[name].keys():
                for j in range(5):
                    feature_vectors[name][test + str(j)] = j * 0.2 <= feature_vectors[name][test] < (j + 1) * 0.2
                del feature_vectors[name][test]

    for name in feature_vectors.keys():

        if TF_IDF_TEST not in feature_vectors[name]:
            if is_tree_based:
                feature_vectors[name][TF_IDF_TEST + "0"] = 1
                for i in range(1, 5):
                    feature_vectors[name][TF_IDF_TEST + str(i)] = 0
            else:
                feature_vectors[name][TF_IDF_TEST] = 0
        feature_vectors[name][IS_NUMERIC] = test_examples_map['is_numeric']
        feature_vectors[name]['name'] = name.encode("utf-8")
        feature_vectors[name]['column_name'] = test_examples_map['name'] + "!" + test_examples_map['semantic_type']
        if is_labeled and name.split("!")[0] == test_examples_map['semantic_type']:
            feature_vectors[name]['label'] = 1
        else:
            feature_vectors[name]['label'] = 0
    return feature_vectors.values()
