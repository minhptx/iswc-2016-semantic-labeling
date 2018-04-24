Semantic Typing
===============

Automatically assign semantics to large data sets from heterogeneous sources based on their features using several Statistical and Machine Learning techniques.


## Prerequisites

1. Elasticsearch
2. Pyspark
3. scikit-learn
4. pandas


## Run API
1. Build docker image

```cd container; docker build -t isi/semantic-labeling .```

2. Start elasticsearch:

```docker-compose up```

3. Calling API

```bin/semantic_labeling.sh <train_dataset> <test_dataset> <train_dataset2>```
