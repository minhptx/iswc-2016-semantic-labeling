import csv
import os
import re
import time
from collections import OrderedDict, defaultdict

from lib import searcher, indexer
from lib.source import Source
from lib.utils import not_allowed_chars
from main import file_write
from main.random_forest import MyRandomForest

__author__ = 'alse'


class SemanticLabeler:
    def __init__(self):
        self.dataset_map = {}
        self.file_class_map = {}
        self.random_forest = None

    def read_data_sources(self, folder_paths):
        for folder_name in folder_paths:
            folder_path = "data/datasets/%s" % folder_name
            source_map = OrderedDict()
            data_folder_path = os.path.join(folder_path, "data")
            model_folder_path = os.path.join(folder_path, "model")

            for filename in os.listdir(data_folder_path):
                extension = os.path.splitext(filename)[1]

                if ".DS" in filename:
                    continue

                print filename

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

    def train_random_forest(self, train_sizes, data_sets):
        self.random_forest = MyRandomForest(data_sets, self.dataset_map)
        self.random_forest.train(train_sizes)

    def train_semantic_types(self, dataset_list):
        for name in dataset_list:
            index_config = {'name': re.sub(not_allowed_chars, "!", name)}
            indexer.init_analyzers(index_config)
            source_map = self.dataset_map[name]
            for idx in range(len(source_map.keys())):
                source = source_map[source_map.keys()[idx]]
                source.save(index_config={'name': re.sub(not_allowed_chars, "!", name)})

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

                        for threshold in [0.01]:
                            found = False
                            rank = 1
                            rank_score = 0
                            for prediction in semantic_types:
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
                                column.name + "\t" + column.semantic_type + "\t" + str(semantic_types) + "\n")
                            file_write.write(str(rank_score) + "\n")
                            rank_score_map[size][threshold] += rank_score
                            count_map[size][threshold] += 1
            running_time = time.time() - start_time
            for threshold in [0.01]:
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
        self.read_class_type_from_csv("data/datasets/%s/classes.csv" % test_set)
        print self.file_class_map.keys()
        rank_score_map = defaultdict(lambda: 0)
        count_map = defaultdict(lambda: 0)

        source_result_map = {}
        train_index_config = {'name': train_set}

        for idx, source_name in enumerate(self.dataset_map[test_set]):
            if source_name not in self.file_class_map:
                continue
            train_examples_map = searcher.search_types_data(train_index_config, [self.file_class_map[source_name]])

            source = self.dataset_map[test_set][source_name]

            column_result_map = {}
            for column in source.column_map.values():

                if not column.semantic_type or not column.value_list or "ontology" not in column.semantic_type:
                    continue

                textual_train_map = searcher.search_similar_text_data(train_index_config, column.value_text,
                                                                      [self.file_class_map[source_name]])

                semantic_types = column.predict_type(train_examples_map, textual_train_map, self.random_forest)

                print column.name

                file_write.write(
                    column.name + "\t" + column.semantic_type + "\t" + str(semantic_types) + "\n")

                for threshold in [0.1, 0.15, 0.2, 0.25, 0.5]:
                    rank = 0
                    found = False
                    rank_score = 0
                    for prediction in semantic_types:
                        if column.semantic_type in prediction[1]:
                            if prediction[0][1] >= threshold:
                                rank_score = 1.0 / (rank + 1)
                            found = True

                        if not found and prediction[0][0] != 0:
                            rank += len(prediction[1])

                    if not found:
                        if semantic_types[0][0][1] < threshold:
                            rank_score = 1
                    file_write.write(str(rank_score) + "\n")
                    rank_score_map[threshold] += rank_score
                    count_map[threshold] += 1

            source_result_map[source_name] = column_result_map

        for threshold in [0.1, 0.15, 0.2, 0.25, 0.5]:
            file_write.write(
                " MRR: " + str(
                    rank_score_map[threshold] * 1.0 / count_map[threshold]) + " Count: " + str(
                    count_map[threshold]) + "\n")
        return source_result_map
