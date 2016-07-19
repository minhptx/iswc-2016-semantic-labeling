from lib.utils import get_index_name

__author__ = "minh"


class Indexer:
    def __init__(self, es):
        self.es = es

    def init_analyzers(self, index_config):
        self.es.indices.create(index=get_index_name(index_config), body={
            "settings": {
                "analysis": {
                    "analyzer": {
                        "textual": {
                            "filter": [
                                "standard",
                                "lowercase",
                                "stop",
                            ],
                            "type": "custom",
                            "tokenizer": "standard"
                        },
                        "number_text": {
                            "filter": [
                                "lowercase",
                                "word_delimiter",
                                "stop",
                            ],
                            "type": "custom",
                            "tokenizer": "standard"
                        },
                        "whitespace_text": {
                            "filter": [
                                "lowercase",
                                "stop",
                                "kstem"
                            ],
                            "type": "custom",
                            "tokenizer": "whitespace"
                        }
                    }
                }
            }
        })

    def index_column(self, column, source_name, index_config):
        body = column.to_json()
        body['source'] = source_name
        self.es.index(index=get_index_name(index_config), doc_type=source_name,
                          body=body)

    def index_source(self, source, index_config):
        # self.es.indices.put_mapping(index=get_index_name(index_config), doc_type=source.index_name, body={
        #     source.index_name: {
        #         "properties": {
        #             "whitespace_textual": {
        #                 "type": "string",
        #                 "analyzer": "whitespace_text"
        #             },
        #             "number_textual": {
        #                 "type": "string",
        #                 "analyzer": "number_text"
        #             }
        #         }
        #     }
        # })

        for column in source.column_map.values():
            if column.semantic_type:
                self.index_column(column, source.index_name, index_config)

    def delete_column(self, index_config):
        if self.es.indices.exists(get_index_name(index_config)):
            self.es.delete(index=get_index_name(index_config))
            return True
        return False
