import re

from main import sc

__author__ = 'minh'
#
not_allowed_chars = '[\/*?"<>|\s\t]'
# numeric_regex = r"\A((\\-)?[0-9]{1,3}(,[0-9]{3})+(\\.[0-9]+)?)|((\\-)?[0-9]*\\.[0-9]+)|((\\-)?[0-9]+)|((\\-)?[0" \
#                 r"-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?)\Z"


is_column_based = True
is_tree_based = False


def split_number_text(example):
    numbers = re.findall(r"(\d+(\.\d+([Ee]\d+)?)?)", example)
    text = re.sub(r"(\d+(\.\d+([Ee]\d+)?)?)", "", example)
    return numbers, text


def get_distribution(data):
    return sc.parallelize(data).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).sortBy(
        lambda x: -x[1]).zipWithIndex().flatMap(
        lambda x: [x[1]] * int(x[0][1] * 100.0 / len(data))).collect()


def get_index_name(index_config):
    if isinstance(index_config, str):
        return index_config.lower()
    return str(index_config['name']).lower()


def get_new_index_name(semantic_type, source_type):
    domain = semantic_type["domain"]["uri"].split("/")[-1]
    _type = semantic_type["type"]["uri"].split("/")[-1]
    name = domain + "---" + _type
    return (name + ":" + source_type).lower()
