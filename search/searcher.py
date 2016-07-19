from elasticsearch.helpers import scan

from lib.utils import get_index_name

__author__ = "minh"


class Searcher:
    def __init__(self, es):
        self.es = es

    def search_columns_data(self, index_config, source_names):
        result = list(scan(self.es, index=get_index_name(index_config), doc_type=','.join(source_names),
                           query={"query": {"match_all": {}}}))

        return result

    def search_similar_text_data(self, index_config, value_text, source_names):
        try:
            text = value_text
            result = self.es.search(index=get_index_name(index_config), doc_type=','.join(source_names),
                                    body={
                                        "query": {
                                            "match": {
                                                "textual": text,
                                            }
                                        }
                                    },
                                    size=10)
        except Exception:
            result = {"hits": {"hits": []}}
        return result

    def search_types_data(self, index_config, source_names):
        return self.search_columns_data(index_config, source_names)
