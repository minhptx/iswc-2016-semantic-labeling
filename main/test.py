import csv
import os
from collections import OrderedDict

# with open("data/datasets/t2d/classes_complete.csv", "r") as f:
#     csv_reader = csv.reader(f)
#     class_list = []
#     for row in csv_reader:
#         class_list.append(row[1].strip())
#     class_list = set(class_list)
# print len(class_list)
# for fname in os.listdir("data/datasets/dbpedia_full/csv"):
#     if ".DS" in fname:
#         continue
#     print fname.split(".")[0]
#     if fname.split(".")[0] not in class_list:
#         os.remove(os.path.join("data/datasets/dbpedia_full/csv", fname))
import pandas as pd

from lib.source import Source

for fname in os.listdir("data/datasets/dbpedia_full/csv"):
    if ".DS" in fname:
        continue
    df = pd.read_csv(os.path.join("data/datasets/dbpedia_full/csv", fname), nrows=1000)
    df.head(1000).to_csv(os.path.join("data/datasets/dbpedia_full/data", fname), mode='w', header=True)

# for filename in os.listdir("data/datasets/dbpedia_full/data"):
#     extension = os.path.splitext(filename)[1]
#
#     print filename
#
#     source = Source(os.path.splitext(filename)[0])
#     file_path = os.path.join("data/datasets/dbpedia_full/data", filename)
#
#     if "full" in filename:
#         source.read_data_from_wc_csv(file_path)
#     elif extension == ".csv":
#         source.read_data_from_csv(file_path)
#     elif extension == ".json":
#         source.read_data_from_json(file_path)
#     elif extension == ".xml":
#         source.read_data_from_xml(file_path)
#     else:
#         source.read_data_from_text_file(file_path)
#
#     source.write_csv_file(os.path.join("data/datasets/dbpedia_full/data2", filename))
