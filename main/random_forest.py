import os

import numpy as np
import pandas as pd
from sklearn.externals import joblib
from sklearn.linear_model import LogisticRegression

from lib import searcher
from lib.utils import is_tree_based
from tests.integrated import feature_list, tree_feature_list


class MyRandomForest:
    def __init__(self, data_sets=None, dataset_map=None, model_path=None):
        self.data_sets = data_sets
        self.dataset_map = dataset_map
        self.model_path = model_path
        self.model = None
        self.feature_selector = None

    def generate_train_data(self, train_sizes):
        train_data = []
        for data_set in self.data_sets:
            train_data = []
            index_config = {'name': data_set}
            source_map = self.dataset_map[data_set]
            double_name_list = source_map.values() * 2
            for size in train_sizes:
                for idx, source_name in enumerate(source_map.keys()):
                    train_names = [source.index_name for source in double_name_list[idx + 1: idx + size + 1]]
                    train_examples_map = searcher.search_types_data(index_config, train_names)
                    source = source_map[source_name]
                    for column in source.column_map.values():
                        if column.semantic_type:
                            textual_train_map = searcher.search_similar_text_data(index_config,
                                                                                  column.value_text,
                                                                                  train_names)
                            feature_vectors = column.generate_candidate_types(train_examples_map, textual_train_map,
                                                                              is_labeled=True)
                            train_data += feature_vectors
        return train_data

    def train(self, train_sizes):
        if os.path.exists(self.model_path):
            print "Loading ..."
            self.model = joblib.load("model/lr.pkl")
        else:
            train_df = self.generate_train_data(train_sizes)
            train_df = pd.DataFrame(train_df)
            train_df = train_df.replace([np.inf, -np.inf, np.nan], 0)
            # self.model = LogisticRegression(n_estimators=200, combination="majority_voting")
            self.model = LogisticRegression(class_weight="balanced")
            # print train_df
            # sample_weight = train_df['label'].apply(lambda x: 15 if x else 1)
            # print sample_weight
            if is_tree_based:
                self.model.fit(train_df[tree_feature_list], train_df['label'])
            else:
                # self.model.fit(train_df[feature_list], train_df['label'])
                self.model.fit(train_df[feature_list], train_df['label'])

                # train_df[feature_list + ["label"]].to_csv("train.csv", mode='w', header=True)
                # cost = len(train_df[train_df['label'] == False]) / len(train_df[train_df['label'] == True])
                # self.model.fit(train_df[feature_list].as_matrix(), train_df['label'].as_matrix(),
                #                np.tile(np.array([1, cost, 0, 0]), (train_df.shape[0], 1)))
            joblib.dump(self.model, self.model_path)

    def predict(self, test_data, true_type):
        test_df = pd.DataFrame(test_data)
        test_df = test_df.replace([np.inf, -np.inf, np.nan], 0)
        if is_tree_based:
            test_df['prob'] = [x[1] for x in self.model.predict_proba(test_df[tree_feature_list].as_matrix())]
        else:
            test_df['prob'] = [x[1] for x in self.model.predict_proba(test_df[feature_list].as_matrix())]
        # test_df['prediction'] = [1 if x else 0 for x in self.model.predict(test_df[feature_list])]
        test_df['truth'] = test_df['name'].map(lambda row: row.split("!")[0] == true_type)
        test_df = test_df.sort_values(["prob"], ascending=[False])

        if os.path.exists("debug.csv"):
            test_df.to_csv("debug.csv", mode='a', header=False)
        else:
            test_df.to_csv("debug.csv", mode='w', header=True)

        return test_df[["prob", 'name']].T.to_dict().values()
