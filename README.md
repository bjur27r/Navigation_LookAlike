# TFMbjur

##AUTOR:
BJU
##Descripción General:
Se trata de un Webservice gestionado en Flask, que da acceso a la gestion de segmentos de datos creados en un back end hecho en Spark-2.0.1 desde los habitos de navegación de los usuarios registrados desde un TMS(tag manager system). El TMS registra para cada usuario entre otros parametros, la URL que este visitó y el UserID assigndo por el TMS.
Estos datos de navegación están registrados en una muestra en el archivo "tmp2.csv" desde donde hace lectura el motor de analisis. Los outputs de la api, son de dos tipo (Clases), por una parte permite gestionar segmentos contruidos dedse URL de forma Disyuntiva y conjuntiva, originando nuevos segmentos de usuarios desde la combinación de URLs, y por otra parte generá amplitud sobre esos segmentos, desde los nuevos que en virtud un modelo de recomendación genera y sobre la base de usuarios totales para diferentes niveles de similaridad medida de acuerdo al angulo entre los vectores de usuarios(cos phi).

##Base:
Spark 2.0.1-Python 2.7

##Librerias:
from flask import Blueprint
import json
from generator import segment_builder, segment_similarity
import logging
logging.basicConfig(level=logging.INFO)
from flask import Flask, request
import time, sys, cherrypy, os
from paste.translogger import TransLogger
from appSeg import create_app
from pyspark import SparkContext, SparkConf

##Ejecución:
Local

##IDE:
Sublime TextEditor
Jupyter notebook Pyspark.2.0.1

##Files:
###Datos Entrada:
"tmp2.csv": contiene los habitos de navegación de 52K usuarios entre 86k paginas vistas.
"/Data/Advertising.csv": al no encontrar resultados concluyentes en "tmp2.csv" para el motor de recomendación , se hizo uso de un archivo con la misma estructura con origen en el libro "Real Machine Learning"

###Ficheros Datos Back End:
"Users_Data.json":Registra todos los usuarios en formato clave(User ID), valor(lista de usuarios con un phi entre 45y 50º)
"segments.json":Registra cada segmento creado, como clave(Nombre Segment), valor(lista de todos los usuarios comprendidos en el segmento desde la conjunción u disyunción de los usuarios (ó visitantes únicos) de las URLś registrada por el TMS.

##Ficheros Back End:
 
Generator.py: motor de analisis compuesto pr dos clases comentadas:
		-segment_builder: Creación y gestión de segmentos.
		-segment_similarity: Creación de un motor e recomendación y funciones de acceso  este.

AppSeg.py: Definición estructura Api.(Se anexa ejemplos de su acceso)
Server2.py:Creación Webserver en 0.0.0.0:8081
Se incuyen dentro de la carpeta Notebooks Pyspark todos los cuadernos de apoyo que han servido para la elaboración del codigo.

##Bibliografia


Big Data Analitycs with Spark-Apress
Machine Learning with spark-Packt
Spark for python developer-Packt
Real World Machine Learning-Manning.
Spark in action -Manning.
Scala in action -Manning.
Flask by exxample-PAckt
Material KSCHOOL MDS.

https://www.codementor.io/jadianes/building-a-recommender-with-apache-spark-python-example-app-part1-du1083qbw
https://www.codementor.io/jadianes/building-a-web-service-with-apache-spark-flask-example-app-part2-du1083854
http://mlnick.github.io/blog/2013/04/01/movie-recommendations-and-more-with-spark/





