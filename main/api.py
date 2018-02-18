#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging
import os
import sys
import ujson

from elasticsearch import Elasticsearch

from main.semantic_labeler import SemanticLabeler

"""API for semantic labeling, a dataset is a set of sources"""


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    formatter = logging.Formatter(
        '>>>>>> %(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(ch)

    return logger


def is_indexed(dataset):
    """Check if this dataset is indexes in ElasticSearch/Spark

    :param dataset: str
    :return: bool
    """
    es = Elasticsearch()
    return es.indices.exists(dataset.lower())


def semantic_labeling(train_dataset, test_dataset, train_dataset2=None):
    """Doing semantic labeling, train on train_dataset, and test on test_dataset.

    train_dataset2 is optionally provided in case train_dataset, and test_dataset doesn't have overlapping semantic types
    For example, given that train_dataset is soccer domains, and test_dataset is weather domains; the system isn't able
    to recognize semantic types of test_dataset because of no overlapping. We need to provide another train_dataset2, which
    has semantic types of weather domains; so that the system is able to make prediction.

    Train_dataset2 is default to train_dataset. (train_dataset is use to train RandomForest)

    :param train_dataset: str
    :param test_dataset: str
    :param train_dataset2: Optional[str]
    :return:
    """
    logger = get_logger("semantic-labeling-api")

    if train_dataset2 is None:
        train_dataset2 = train_dataset
        datasets = [train_dataset, test_dataset]
    else:
        datasets = [train_dataset, test_dataset, train_dataset2]

    semantic_labeler = SemanticLabeler()
    # read data into memory
    logger.info("Read data into memory")
    semantic_labeler.read_data_sources(datasets)
    # index datasets that haven't been indexed before

    not_indexed_datasets = [dataset for dataset in datasets if not is_indexed(dataset)]
    if len(not_indexed_datasets) > 0:
        logger.info("Index not-indexed datasets: %s" % ",".join(not_indexed_datasets))
        semantic_labeler.train_semantic_types(not_indexed_datasets)

    # remove existing file
    if os.path.exists("model/lr.pkl"):
        os.remove("model/lr.pkl")

    # train the model
    logger.info("Train randomforest... with args ([1], [%s]", train_dataset)
    semantic_labeler.train_random_forest([1], [train_dataset])

    # generate semantic typing
    logger.info("Generate semantic typing using: trainset: %s, for testset: %s", train_dataset, test_dataset)
    result = semantic_labeler.test_semantic_types_from_2_sets(train_dataset2, test_dataset)

    if not os.path.exists("output"):
        os.mkdir("output")
    with open("output/%s_result.json" % test_dataset, "w") as f:
        ujson.dump(result, f)

    return result


if __name__ == '__main__':
    if len(sys.argv[1:]) == 3:
        train_dataset, test_dataset, train_dataset2 = sys.argv[1:]
    elif len(sys.argv[1:]) == 2:
        train_dataset, test_dataset = sys.argv[1:]
        train_dataset2 = None
    else:
        assert False, "Invalid arguments: %s" % sys.argv

    logger = get_logger("api-starter")
    logger.info("Calling semantic labeling API with args: %s, %s, %s" % (train_dataset, test_dataset, train_dataset2))
    semantic_labeling(train_dataset, test_dataset, train_dataset2)
