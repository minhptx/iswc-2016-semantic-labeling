import csv
import json
import logging
import os
import re
import time
from collections import OrderedDict, defaultdict

from lib import searcher, indexer
from lib.column import Column
from lib.source import Source
from lib.utils import not_allowed_chars
from main import file_write
from main.logutil import get_logger
from main.random_forest import MyRandomForest

__author__ = 'alse'


class SemanticLabeler:

    logger = get_logger("SemanticLabeler", level=logging.DEBUG)

    def __init__(self):
        self.dataset_map = {}
        self.file_class_map = {}
        self.random_forest = None

    def preprocess_memex_data_sources(self, folder_path):
        source_map = OrderedDict()
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            print file_path
            with open(file_path, "r") as f:
                for json_line in f.readlines():
                    json_obj = json.loads(json_line)
                    source_name = json_obj["tld"]

                    if source_name not in source_map:
                        source_map[source_name] = Source(source_name)

                    source = source_map[source_name]

                    for attr in json_obj:
                        if attr.startswith("inferlink"):
                            attr_name = attr.split("_")[1]
                            if attr_name not in source.column_map:
                                source.column_map[attr_name] = Column(attr_name, source.name)
                                source.column_map[attr_name].semantic_type = attr_name
                            for ele1 in json_obj[attr]:
                                if isinstance(ele1["result"], dict):
                                    source.column_map[attr_name].add_value(ele1["result"]["value"])
                                else:
                                    for ele2 in ele1["result"]:
                                        source.column_map[attr_name].add_value(ele2["value"])

        for source in source_map.values():
            if source.column_map:
                source.write_csv_file("data/datasets/memex/%s" % source.name)

    def read_data_sources(self, folder_paths):
        semantic_type_set = set()
        attr_count = 0
        for folder_name in folder_paths:
            self.logger.debug("Read dataset: %s", folder_name)

            folder_path = "data/datasets/%s" % folder_name
            source_map = OrderedDict()
            data_folder_path = os.path.join(folder_path, "data")
            model_folder_path = os.path.join(folder_path, "model")

            for filename in sorted(os.listdir(data_folder_path)):
                extension = os.path.splitext(filename)[1]

                if ".DS" in filename:
                    continue

                self.logger.debug("    -> read: %s", filename)

                source = Source(os.path.splitext(filename)[0])
                file_path = os.path.join(data_folder_path, filename)

                if "full" in data_folder_path:
                    source.read_data_from_wc_csv(file_path)
                elif extension == ".csv":
                    source.read_data_from_csv(file_path)
                elif extension == ".json":
                    source.read_data_from_json(file_path)
                elif extension == ".xml":
                    source.read_data_from_xml(file_path)
                else:
                    source.read_data_from_text_file(file_path)
                source_map[filename] = source

                # NOTE: BINH delete empty columns here!!!, blindly follows the code in indexer:36
                for key in list(source.column_map.keys()):
                    column = source.column_map[key]
                    if column.semantic_type:
                        if len(column.value_list) == 0:
                            del source.column_map[key]
                            source.empty_val_columns[key] = column
                            logging.warning("Indexer: IGNORE COLUMN `%s` in source `%s` because of empty values",
                                            column.name, source.name)

                for column in source.column_map.values():
                    semantic_type_set.add(column.semantic_type)
                attr_count += len(source.column_map.values())
            if os.path.exists(model_folder_path):
                for filename in os.listdir(model_folder_path):
                    if ".DS" in filename:
                        continue

                    try:
                        source = source_map[os.path.splitext(os.path.splitext(filename)[0])[0]]
                    except:
                        source = source_map[filename]

                    extension = os.path.splitext(filename)[1]
                    if extension == ".json":
                        source.read_semantic_type_json(os.path.join(model_folder_path, filename))
                    else:
                        print source
                        source.read_semantic_type_from_gold(os.path.join(model_folder_path, filename))

            self.dataset_map[folder_name] = source_map
            print semantic_type_set
            print len(semantic_type_set)
            print attr_count

    def train_random_forest(self, train_sizes, data_sets):
        self.random_forest = MyRandomForest(data_sets, self.dataset_map, "model/lr.pkl")
        self.random_forest.train(train_sizes)

    def train_semantic_types(self, dataset_list):
        for name in dataset_list:
            self.logger.debug("Indexing dataset %s", name)

            index_config = {'name': re.sub(not_allowed_chars, "!", name)}
            indexer.init_analyzers(index_config)
            source_map = self.dataset_map[name]
            for idx, key in enumerate(source_map.keys()):
                source = source_map[key]
                source.save(index_config={'name': re.sub(not_allowed_chars, "!", name)})
                self.logger.debug("    + finish index source: %s", key)

    def predict_semantic_type_for_column(self, column):
        train_examples_map = searcher.search_types_data("index_name", [])
        textual_train_map = searcher.search_similar_text_data("index_name", column.value_text, [])
        return column.predict_type(train_examples_map, textual_train_map, self.random_forest)

    def test_semantic_types(self, data_set, test_sizes):
        rank_score_map = defaultdict(lambda: defaultdict(lambda: 0))
        count_map = defaultdict(lambda: defaultdict(lambda: 0))

        index_config = {'name': data_set}
        source_map = self.dataset_map[data_set]
        double_name_list = source_map.values() * 2
        file_write.write("Dataset: " + data_set + "\n")
        for size in test_sizes:
            start_time = time.time()

            for idx, source_name in enumerate(source_map.keys()):
                train_names = [source.index_name for source in double_name_list[idx + 1: idx + size + 1]]
                train_examples_map = searcher.search_types_data(index_config, train_names)
                source = source_map[source_name]

                for column in source.column_map.values():
                    if column.semantic_type:
                        textual_train_map = searcher.search_similar_text_data(index_config, column.value_text,
                                                                              train_names)
                        semantic_types = column.predict_type(train_examples_map, textual_train_map, self.random_forest)

                        for threshold in [0.0]:
                            found = False
                            rank = 1
                            rank_score = 0
                            for prediction in semantic_types[:1]:
                                if column.semantic_type in prediction[1]:
                                    if prediction[0] > threshold and prediction[0] != 0:
                                        rank_score = 1.0 / (rank)
                                    found = True
                                    break
                                if prediction[0] != 0:
                                    rank += len(prediction[1])

                            if not found and semantic_types[0][0] < threshold:
                                rank_score = 1
                            # file_write.write(
                            #     column.name + "\t" + column.semantic_type + "\t" + str(semantic_types) + "\n")
                            file_write.write(str(rank_score) + "\n")
                            rank_score_map[size][threshold] += rank_score
                            count_map[size][threshold] += 1
            running_time = time.time() - start_time
            for threshold in [0.0]:
                file_write.write(
                    "Size: " + str(size) + " F-measure: " + str(
                        rank_score_map[size][threshold] * 1.0 / count_map[size][threshold]) + " Time: " + str(
                        running_time) + " Count: " + str(count_map[size][threshold]) + "\n")

    def read_class_type_from_csv(self, file_path):
        self.file_class_map = {}
        with open(file_path, "r") as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                self.file_class_map[row[0].replace(".tar.gz", ".csv")] = row[1]

    def test_semantic_types_from_2_sets(self, train_set, test_set):
        # self.read_class_type_from_csv("data/datasets/%s/classes.csv" % test_set)
        # print self.file_class_map.keys()
        rank_score_map = defaultdict(lambda: 0)
        count_map = defaultdict(lambda: 0)

        source_result_map = {}
        train_index_config = {'name': train_set}
        train_names = [source.index_name for source in self.dataset_map[train_set].values()]

        for idx, source_name in enumerate(self.dataset_map[test_set]):
            # if source_name not in self.file_class_map:
            #     continue
            train_examples_map = searcher.search_types_data(train_index_config,
                                                            train_names)

            source = self.dataset_map[test_set][source_name]

            column_result_map = {}
            for column in source.column_map.values():
                # if not column.semantic_type or not column.value_list or "ontology" not in column.semantic_type:
                #     continue
                if not column.semantic_type or not column.value_list:
                    continue

                textual_train_map = searcher.search_similar_text_data(train_index_config, column.value_text,
                                                                      train_names)

                semantic_types = column.predict_type(train_examples_map, textual_train_map, self.random_forest)
                column_result_map[column.name] = semantic_types
                print column.name

                file_write.write(
                    column.name + "\t" + column.semantic_type + "\t" + str(semantic_types) + "\n")

                for threshold in [0.0, 0.1, 0.15, 0.2, 0.25, 0.5]:
                    found = False
                    rank = 1
                    rank_score = 0
                    for prediction in semantic_types[:1]:
                        if column.semantic_type in prediction[1]:
                            if prediction[0] > threshold and prediction[0] != 0:
                                rank_score = 1.0 / rank
                            found = True
                            break
                        if prediction[0] != 0:
                            rank += len(prediction[1])

                    if not found and semantic_types[0][0] < threshold:
                        rank_score = 1
                    file_write.write(str(rank_score) + "\n")
                    rank_score_map[threshold] += rank_score
                    count_map[threshold] += 1

            source_result_map[source_name] = column_result_map

        for threshold in [0.0, 0.1, 0.15, 0.2, 0.25, 0.5]:
            file_write.write(
                " MRR: " + str(
                    rank_score_map[threshold] * 1.0 / count_map[threshold]) + " Count: " + str(
                    count_map[threshold]) + " threshold=" + str(threshold) + "\n")
        return source_result_map

    def write_data_for_transform(self, name):
        for source_name, source in self.dataset_map[name].items():
            for attribute in source.column_map.values():
                attribute.write_to_data_file()
