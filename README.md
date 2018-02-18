Semantic Typing
===============

Automatically assign semantics to large data sets from heterogeneous sources based on their features using several Statistical and Machine Learning techniques.


## Prerequisites

1. Elasticsearch
2. Pyspark
3. scikit-learn
4. pandas


## Run API

1. Start elasticsearch:

```cd container; docker-compose up```

2. Calling API

```bin/semantic_labeling.sh <train_dataset> <test_dataset> <train_dataset2>```
