from flask import Flask, request
from flask import make_response

from lib import indexer
from lib.column import Column
from lib.source import Source
from lib.utils import get_new_index_name
from main.semantic_labeler import SemanticLabeler

__author__ = 'alse'

service = Flask(__name__)

SEMANTIC_TYPE_URL = "/semantic_type"
COLUMN_URL = "/column"
FIRST_TIME_URL = "/ftu"
UPLOAD_FOLDER = "/data/"  # TODO change this to model folder
TEST_URL = "/test"
semantic_labeler = SemanticLabeler()


def error(message=""):
    with service.app_context():
        response = make_response()
        response.status_code = 500
        response.headers = {
            "X-Status-Reason": message
        }
        return response


def allowed_file(filename, extensions):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in extensions


@service.route(SEMANTIC_TYPE_URL, methods=['POST', 'PUT'])
def add_semantic_type(column=None, semantic_type=None):
    if not (column and semantic_type):
        column = request.json["column"]
        semantic_type = request.json["semantic_type"]


    column_name = column.keys()[0]

    if column and semantic_type and column_name:
        source = Source(column_name)
        source.read_data_from_dict(column)
        source.set_semantic_type(semantic_type, column_name)
        _id = get_new_index_name(semantic_type, column_name)
        source.save(index_config={"name": _id, "size": 0})
        return str(_id)
        """
    try:
        if not (column and semantic_type):
            column = request.json["column"]
            semantic_type = request.json["semantic_type"]
        column_name = column.keys()[0]

        if column and semantic_type and column_name:
            source = Source(column_name)
            source.read_data_from_dict(column)
            source.set_semantic_type(semantic_type, column_name)
            _id = get_new_index_name(semantic_type, column_name)
            source.save(index_config={"name": _id, "size": 0})
            return str(_id)
    except Exception as e:
        return error(e.message+" "+str(e.args))"""


@service.route(SEMANTIC_TYPE_URL, methods=["DELETE"])
def delete_semantic_type():
    semantic_type = request.json["semantic_type"]
    _id = get_new_index_name(semantic_type, "*")
    if not indexer.delete_column(index_config={"name": _id, "size": 0}):
        return error("Unable to delete semantic type.")


# NOTE. This is to add many columns to a given semantic type in a bulk
@service.route(SEMANTIC_TYPE_URL + "/bulk", methods=['POST'])
def add_semantic_type_bulk():
    semantic_type = request.json["semantic_type"]
    columns = request.json["columns"]

    for column in columns:
        add_semantic_type(column, semantic_type)

    return ""


@service.route(COLUMN_URL, methods=["DELETE"])
def delete_column():
    semantic_type = request.json["semantic_type"]
    column_name = request.json["column_name"]
    _id = get_new_index_name(semantic_type, column_name)
    if not indexer.delete_column(index_config={"name": _id, "size": 0}):
        return error("Unable to delete semantic type.")


@service.route(COLUMN_URL, methods=["POST"])
def get_semantic_type():
    try:
        column_json = request.json["column"]
        header = column_json.keys()[0]
        column = Column(header)
        for element in column_json[header]:
            column.add_value(element)

        return str(semantic_labeler.predict_semantic_type_for_column(column))
    except Exception as e:
        return error(e.message+" "+str(e.args))


@service.route(FIRST_TIME_URL, methods=["GET"])
def first_time():
    try:
        semantic_labeler.read_data_sources(["soccer", "dbpedia", "museum2","flights", "weather", "phone"])
        semantic_labeler.train_semantic_types(["soccer", "dbpedia", "museum2", "flights", "weather", "phone"])
        semantic_labeler.train_random_forest([11], ["soccer"])
        return "Training Complete"
    except Exception as e:
        return error(e.message+" "+str(e.args))


@service.route(TEST_URL, methods=["GET"])
def test_service():
    try:
        semantic_labeler.read_data_sources(["soccer", "weather"])
        semantic_labeler.train_random_forest([5], ["soccer"])
        semantic_labeler.test_semantic_types("weather", [3])
        return "Tests complete"
    except Exception as e:
        return error("Test failed due to: "+e.message+" "+str(e.args))

if __name__ == "__main__":
    service.run(debug=True, port=8000)
