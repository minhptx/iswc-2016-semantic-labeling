import locale
import os
import re
from collections import defaultdict

from numpy.random import choice

from lib.utils import split_number_text, not_allowed_chars, get_distribution
from tests.integrated import get_test_results

__author__ = 'alse'


class Column:
    def __init__(self, name, source_name=None):
        self.source_name = source_name
        self.name = name.replace("#", "")
        self.value_list = []
        self.textual_list = []
        self.textual_set = set()
        self.word_set = set()
        self.semantic_type = None
        self.numeric_list = []
        self.sample_list = []
        self.value_text = ""
        self.is_prepared = False
        self.word2vec = []
        self.word_lengths = []
        self.char_lengths = []
        self.histogram_list = []

    def write_to_data_file(self):
        if not os.path.isdir("data/transform_data/%s" % self.semantic_type):
            os.makedirs("data/transform_data/%s" % self.semantic_type)
        with open("data/transform_data/%s/%s" % (self.semantic_type, os.path.basename(self.source_name)), "w") as f:
            f.write("\n".join(self.value_list))

    def add_value(self, value):
        if not value:
            return

        value = value.strip()

        if not value or value == "NULL":
            return

        value = value.decode('utf-8').encode('ascii', 'ignore')
        # try:
        #     value = value.encode("ascii", "ignore")
        # except:
        #     value = value.decode("unicode_escape").encode("ascii", "ignore")

        value = re.sub(not_allowed_chars, " ", value)

        self.word_set = self.word_set.union(set(value.split(" ")))

        if "full" in self.source_name and len(self.value_list) > 500:
            return
        self.value_list.append(value)

        self.word_lengths.append(len(value.split(" ")))
        self.char_lengths.append(len(value))

        numbers, text = split_number_text(value)

        if text:
            self.value_text += (" " + text)

            self.textual_set.add(text)
            self.textual_list.append(text)

        if numbers:
            self.numeric_list.extend([locale.atof(num[0]) for num in numbers])

    def prepare_data(self):
        if not self.is_prepared:
            sample_size = min(200, len(self.numeric_list))
            # print self.value_list
            # if percentile(array(self.word_lengths), 25) != percentile(array(self.word_lengths), 75):
            #     self.word_lengths = []
            # if percentile(array(self.char_lengths), 25) != percentile(array(self.char_lengths), 75):
            #     self.char_lengths = []
            if self.numeric_list:
                self.sample_list = choice(self.numeric_list, sample_size).tolist()
            self.histogram_list = get_distribution(self.value_list)
            if len(self.histogram_list) > 20:
                self.histogram_list = []
            # print self.histogram_list

            self.is_prepared = True

    def to_json(self):
        self.prepare_data()
        doc_body = {'source': self.source_name, 'name': self.name, 'semantic_type': self.semantic_type,
                    'textual_set': list(self.textual_set), "textual_list": self.textual_list, "values": self.value_list,
                    'sample_list': self.sample_list, 'textual': self.value_text, 'is_numeric': self.is_numeric(),
                    'word2vec': self.word2vec, 'numeric_list': self.numeric_list, 'char_lengths': self.char_lengths,
                    "word_lengths": self.word_lengths, "histogram": self.histogram_list}
        return doc_body

    def read_json_to_column(self, json_obj):
        self.name = json_obj['name']
        self.semantic_type = json_obj['semantic_type']
        self.value_list = json_obj['values']
        self.histogram_list = json_obj['histogram']
        self.numeric_list = json_obj['numeric']
        self.numeric_count = len(self.numeric_list)
        self.sample_list = json_obj['sample_numeric']
        self.value_text = json_obj['textual']

    def is_numeric(self):
        return len(self.textual_list) * 1.0 / (len(self.textual_list) + len(self.numeric_list))

    def predict_type(self, train_examples_map, textual_train_map, model):
        feature_vectors = self.generate_candidate_types(train_examples_map, textual_train_map)
        predictions = model.predict(feature_vectors, self.semantic_type)
        predictions = [
            ((round(prediction['prob'], 2), prediction['prob'], self.source_name + "!" + self.name),
             prediction['name'].split("!")[0])
            for prediction in predictions]
        prediction_map = defaultdict(lambda: [])
        for prediction in predictions:
            prediction_map[prediction[0][0]].append(prediction[1])
        return sorted(prediction_map.items(), reverse=True)

    def generate_candidate_types(self, train_examples_map, textual_train_map, is_labeled=False):
        return get_test_results(train_examples_map, textual_train_map, self.to_json(), is_labeled)
