
#Bloque importacion de librerias

from flask import Blueprint
main = Blueprint('main', __name__)
import json
from generator import segment_builder
 import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from flask import Flask, request


#Me devuelve todos la serie temporal de usuarios nuevos.

@main.route("/12545/<string:host>/time_serie", methods=["GET"])
def segment_time_serie(host):
	segment_ts = segment_builder_app.segment_ts(host,users=False)
	return json.dumps(segment_ts)

#Me devuelve todos los usuarios disponibles para un host determinado.
#http://0.0.0.0:8081/12545/www.cuantarazon.com/time_serie
@main.route("/12545/<string:host>/users", methods=["GET"])
def segment_users(host):
	segment_ts = segment_builder_app.segment_ts(host,users=True)
	return json.dumps(segment_ts)

#Me devuelve todos los hosts disponibles.
#http://0.0.0.0:8081/12545/segments
@main.route("/12545/segments", methods=["GET"])
def segment_unique():
	segments = segment_builder_app.segments_uni()
	return json.dumps(segments)

#Generar un nuevo segmento desde la agregación conjuntiva o disyuntiva entre otros dos.
#Next Upgrade: Flexible.
#name: Nombre Segmento.
#host1: Primer host.
#host2: Segundo host.
#conj{1:Disyuntivo,0:Conjuntivo}
#save {1:Disyuntivo,0:Conjuntivo}
#http://0.0.0.0:8081/12545/segments/new/pruebas/www.cuantocabron.com/www.cuantarazon.com/1/1
@main.route("/12545/segments/new/<string:name>/<string:host1>/<string:host2>/<int:conj>/<int:save>", methods=["GET"])
def segment_mix(host1,host2,name, conj, save):
	segments_1 = segment_builder_app.segment_mix(host1,host2,name, conj, save)
	return json.dumps(segments_1)

def create_app(spark_context, dataset_path):
    #declaro segment builder global

    global segment_builder_app 

    segment_builder_app = segment_builder(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app