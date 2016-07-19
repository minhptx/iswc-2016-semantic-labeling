from elasticsearch import Elasticsearch

from search.indexer import Indexer
from search.searcher import Searcher

__author__ = 'alse'
elastic_search = Elasticsearch()
indexer = Indexer(elastic_search)
searcher = Searcher(elastic_search)
