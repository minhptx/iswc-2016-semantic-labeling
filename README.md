Semantic Typing
===============

Automatically assign semantics to large data sets from heterogeneous sources based on their features using several Statistical and Machine Learning techniques.


##Prerequisites

1. Elasticsearch
2. Pyspark
3. scikit-learn
4. pandas

##API
###First Time User

This has to be called the very first time the service is setup.

**URL** ```/ftu```

**Method:** GET

**Parameters:** None

###Add Semantic Type

This adds a new semantic type and the corresponding column.

**URL** ```/semantic_type```

**Method:** POST

| Parameter | Description | Required |
| --------- | ----------- | -------- |
| column  | column data | Yes |
| semantic_type  | semantic type including domain and type | Yes |

**Sample Payload:**

```
"semantic_type": {
  "domain": {
    "uri": "http://erlangen-crm.org/current/E21_Person"
    },
    "type": {
     "uri": "http://isi.edu/integration/karma/dev#classLink"
   },
},
"column": {
  "header": [...Rows in the column...]
}
```
###Add columns to semantic type in bulk

**URL** ```/semantic_type/bulk```

**Method:** POST

| Parameter | Description | Required |
| --------- | ----------- | -------- |
| columns  | column data | Yes |
| semantic_type  | semantic type including domain and type | Yes |

**Sample Payload:**

```
"semantic_type": {
  "domain": {
    "uri": "http://erlangen-crm.org/current/E21_Person"
    },
    "type": {
     "uri": "http://isi.edu/integration/karma/dev#classLink"
   },
},
"columns": {
  "header1": [...Rows in the column...],
  "header2": [...Rows in the column...],
  "header3": [...Rows in the column...],
  "header4": [...Rows in the column...],
}
```

###Delete Semantic Type

Deletes semantic type and all the corresponding column

**URL** ```/semantic_type```

**Method:** DELETE

| Parameter | Description | Required |
| --------- | ----------- | -------- |
| semantic_type  | semantic type including domain and type | Yes |

**Sample Payload:**

```
"semantic_type": {
  "domain": {
    "uri": "http://erlangen-crm.org/current/E21_Person"
    },
    "type": {
     "uri": "http://isi.edu/integration/karma/dev#classLink"
   },
}
```

###Delete column

Delete a column from a semantic type

**URL** ```/column```

**Method:** DELETE

| Parameter | Description | Required |
| --------- | ----------- | -------- |
| column_name  | Name of the column that has to be deleted | Yes |
| semantic_type  | semantic type including domain and type | Yes |

**Sample Payload:**

```
"semantic_type": {
  "domain": {
    "uri": "http://erlangen-crm.org/current/E21_Person"
    },
    "type": {
     "uri": "http://isi.edu/integration/karma/dev#classLink"
   },
},
"column_name": "header1"
}
```

###Get Semantic Type

Determine semantic type of a column

**URL** ```/column```

**Method:** POST

| Parameter | Description | Required |
| --------- | ----------- | -------- |
| column  | column data | Yes |

**Sample Payload:**

```
"column": {
  "header": [...Rows in the column...]
}
```
