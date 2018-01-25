from elasticsearch.helpers import scan

from lib.utils import get_index_name

__author__ = "minh"


class Searcher:
    def __init__(self, es):
        self.es = es

    def search_columns_data(self, index_config, source_names):
        if source_names:
            result = list(scan(self.es, index=get_index_name(index_config), doc_type='service',
                               query={"query": {
                                   "constant_score": {
                                       "filter": {
                                           "terms": {
                                               "source": source_names
                                           }
                                       }
                                   }
                               }}))
        else:
            result = list(scan(self.es, index=get_index_name(index_config), doc_type='service',
                               query={"query": {"match_all": ""}}
                               ))
        return result

    def search_similar_text_data(self, index_config, value_text, source_names):
        try:
            text = value_text
            if source_names:
                result = self.es.search(index=get_index_name(index_config), doc_type='service',
                                        body={
                                            "query": {
                                                "bool": {
                                                    "must": {
                                                        "match": {
                                                            "textual": text
                                                        }
                                                    },
                                                    "filter": {
                                                        "terms": {
                                                            "source": source_names
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        size=10)
            else:
                result = self.es.search(index=get_index_name(index_config), doc_type='service',
                                        body={
                                            "query": {
                                                "match": {
                                                    "textual": text
                                                }
                                            }
                                        },
                                        size=10)
        except Exception:
            result = {"hits": {"hits": []}}
        print("result", result)
        return result

    def search_types_data(self, index_config, source_names):
        return self.search_columns_data(index_config, source_names)
