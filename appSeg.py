from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from generator import segment_builder
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request



@main.route("/12545/<string:host>/time_serie", methods=["GET"])
def segment_time_serie(host):
	segment_ts = segment_builder_app.segment_ts(host,users=False)
	return json.dumps(segment_ts)

#http://0.0.0.0:8081/12545/www.cuantarazon.com/time_serie
@main.route("/12545/<string:host>/users", methods=["GET"])
def segment_users(host):
	segment_ts = segment_builder_app.segment_ts(host,users=True)
	return json.dumps(segment_ts)

#http://0.0.0.0:8081/12545/
@main.route("/12545/segments", methods=["GET"])
def segment_unique():
	segments = segment_builder_app.segments_uni()
	return json.dumps(segments)

def create_app(spark_context, dataset_path):
    #declaro segment builder global

    global segment_builder_app 

    segment_builder_app = segment_builder(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app