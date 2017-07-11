# tested with python2.7 and 3.4
#!/usr/bin/env python
# -*- coding: utf-8 -*- 

#Bloque importacion de librerias
from pyspark import Row
from pyspark.sql.functions import col,udf, unix_timestamp, StringType
from pyspark.sql.types import DateType
import pandas as pd
from pyspark import SparkContext 
from pyspark.sql import SQLContext
import json
import sys
import math
#Bloque definicion funciones
def logs2(l):
		fields = l.split(',')
		visitor = fields[2]
		url = fields[9]
		action  = fields[10]
		pais  = fields[11]
		provincia  = fields[12]
		time = fields[5]
		host = fields[18]
		return (visitor, url, action, pais, provincia, time,host,1)


#Bloque Clases


class segment_builder:
	"""Clase gestora de segmentos
	"""
	
	#Contructura, 
	def __init__(self, sc, data_path):
		self.sc = sc
		self.data_path = data_path
		#sc = SparkContext.getOrCreate()
		self.sqlContext = SQLContext(sc)
		#incluir aqui repositorio a S3
		
		bigT = sc.textFile(data_path,2)
		bigTT = bigT.map(logs2)
		rows = bigTT.map(lambda x: Row(visitorID=x[0], url =x[1], action=x[2], pais=x[3],provincia = x[4],time=x[5]))
		dailyMaster = self.sqlContext.createDataFrame(rows)
		ndf = dailyMaster.withColumn('_1', dailyMaster['time'].cast(DateType()))
		ndf2 = ndf.withColumn('_1', dailyMaster['time'].cast(DateType()))

		def url2(x):
				try:
						a = x.split('//')[1]
	
				except:
		
						a = "0"
	
				return a


		udf2 = udf(lambda x:url2(x) , StringType())

		def hosta(x):
				try:
					a = x.split('/')[0]
	
				except:
		
					a = "0"
	
				return a


		udf3 = udf(lambda x:hosta(x) , StringType())

		def path(x,n):
				try:
					a = x.split('/')[n]
	
				except:
		
					a = "0"
	
				return a

		#vamos recuperando todas las componentes de la url
		udf4 = udf(lambda x:path(x,1) , StringType())
		udf5 = udf(lambda x:path(x,2) , StringType())
		udf6 = udf(lambda x:path(x,3) , StringType())
		udf7 = udf(lambda x:path(x,4) , StringType())

		ndf_url = ndf2.withColumn('urlClean',udf2(ndf2.url))
		ndf_host = ndf_url.withColumn('host', udf3(ndf_url.urlClean))
		ndf_path1 = ndf_host .withColumn('path1', udf4(ndf_host.urlClean))
		self.ndf5 = ndf_path1.withColumn('path2', udf5(ndf_path1.urlClean))
		
	def segment_ts(self, host, users = True):

		
		self.sqlContext.registerDataFrameAsTable(self.ndf5,'ndf5')
		hostQ= host
		if users :
			query = """	SELECT DISTINCT visitorID FROM ndf5 WHERE host ='%s'""" %hostQ 
		else:	

			query = """SELECT _1,count(*) AS visitors FROM (SELECT FIRST(_1) as _1 FROM ndf5 WHERE host ='%s' GROUP BY visitorID) GROUP BY _1 ORDER BY _1"""%hostQ 

		output = self.sqlContext.sql(query)
		df = output.toPandas()
		df = df.dropna(how='all')
		df = df[1:len(df)]
		df2 = df.to_json(orient='records')
		return df2
		#query = """SELECT visitorId,count(*) AS visitors  FROM ndf5 WHERE host ='%s' GROUP BY visitorID) GROUP BY _1 ORDER BY _1"""%hostQ 
		
		#query = """SELECT _1,count(*) AS visits FROM ndf5  GROUP BY _1 ORDER BY _1"""
		
	def segments_uni(self):

		
		self.sqlContext.registerDataFrameAsTable(self.ndf5,'ndf5')
		
		query = """	SELECT DISTINCT host FROM ndf5 """  
	
		output = self.sqlContext.sql(query)
		df = output.toPandas()
		df = df.dropna(how='all')
		df = df[1:len(df)]
		df2 = df.to_json(orient='records')
		return df2
	
	def segment_mix(self, host1,host2,name, conj, save):

		self.sqlContext.registerDataFrameAsTable(self.ndf5,'ndf5')
		
		if conj==1 :
			query = """	SELECT DISTINCT visitorID FROM ndf5 WHERE (host ='%s' AND host ='%s') """ %(host1,host2) 
		else:	

			query = """	SELECT DISTINCT visitorID FROM ndf5 WHERE (host ='%s' OR host ='%s') """ %(host1,host2) 

		output = self.sqlContext.sql(query)
		df = output.toPandas()
		df = df.dropna(how='all')
		df = df[1:len(df)]
		vis_ids = df['visitorID'].tolist()
		len_vis_ids = len(vis_ids)
		segm_def = {"name":name,"IDs":vis_ids, "size":len_vis_ids}

		df2 = segm_def

		if save==1:
			with open('segments.json') as data_file:
				data = json.load(data_file)
				data.append(segm_def)
			with open('segments.json', 'w') as outfile:
				json.dump(data, outfile, sort_keys = True, indent = 4, ensure_ascii = False)
			
			
		return df2


class segment_similarity:
	"""Clase gestora de segmentos
	"""
	
	#Contructura, 
	def __init__(self,sc, data_path):
		self.sc = sc
		self.data_path = data_path




	def build_up_model(self):
		###CARGA DATOS, usamos otra fuente  QUE TIENE LA MISMA ESTRUCTURA (UserId,Publisher Id),con fuente
		Logs = self.sc.textFile('Data/advertising.csv')
		header = Logs.first()
		#quito headers
		Lorgs_norm = Logs.filter(lambda row: row != header).map(lambda l: l.split(','))
		#Para el calculo , me quedo solo con aquellos usuarios que hayan recibido la impresion original
		#agrupamos por publisher, y sacamos el numero de usuarios por publisher, de la forma (publisher, numero de usuarios)

		Log2Rec = Lorgs_norm.map(lambda tokens: (int(tokens[2]),int(tokens[1]),1 if tokens[5] == "True" else 0)).cache()	
		#agrupamos por usuario, y sacamos el numero de interacciones por publisher, de la forma (usuario, numero de interacciones con publishers)
		num_InterPerUser = Log2Rec.groupBy(lambda (x,y,z): x).map(lambda (x,iterator):(x,len(iterator)))
		#filtro los que tengan menos de dos interaccion al no ser relevantes
		num_InterPerUser_fil = num_InterPerUser.filter(lambda (x,y): y>2) 	
		#agrupamos por usuario, desde key y hacemos el join para tener ( usuario,publisher, imp, volumen de interacciones por usuario)
		ints_sWithSize = Log2Rec.groupBy(lambda (x,y,z): x).join(num_InterPerUser_fil).flatMap(lambda(a,(b,c)):((x,c) for x in b))	
		#Hago pares usuario a usuario desde la interraccion por publisher
		ints_sWithSize_2 = ints_sWithSize.map(lambda((a,b,c),d):(b,(a,c,d)))
		#reasigno key al publisher, y hago un join , de cara a ver cada una de las interecciones comunes entre usuarios, usuario a usuario
		#(publisher, usuario 1, usuario 2)	
		inter_pairs_pub = ints_sWithSize_2.join(ints_sWithSize_2)
		#me quito duplicidades y los que son iguales, buscamos tener copres entre peliculas para cada usuario
		ratingPairs = inter_pairs_pub.map(lambda (x,((a,b,c),(a1,b1,c1))):(x,((int(a),int(b),c),(int(a1),int(b1),c1)))).filter(lambda (x,((a,b,c),(a1,b1,c1))):a<a1)
		#calculo las componentes para despues contruir las medidas de similaridad entre cada uno de los pares
		vectorial_params = ratingPairs.map(lambda (a,((y,z,k),(y1,z1,k1))):((y,y1),(z*z1, z,z1,z*z,z1*z1,k,k1,1)) )
		#reduzco los pares 
		vectorial_params_3 = vectorial_params.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1],x[2]+y[2],x[3]+y[3],x[4]+y[4],x[5]+y[5],x[6]+y[6],x[7]+y[7]))

		def correlation(size, dotProduct, ratingSum,rating2Sum,ratingNormSq,rating2NormSq):
			numerator = size * dotProduct - ratingSum * rating2Sum
			denominator = math.sqrt(size * ratingNormSq - ratingSum * ratingSum)*math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)
			if denominator == 0:
				corr =0 
			else:
				corr = numerator / denominator
			return  corr

		def cosineSimilarity(dotProduct,ratingNorm,rating2Norm): 
			if ratingNorm * rating2Norm == 0:
				cosi = 0
			else: 
				cosi = dotProduct / (ratingNorm * rating2Norm)

			return cosi
		def loop4(chunk):
			a = []
			for x in chunk:
				a.append(x)
			return a

		#simmil_metrics_2= vectorial_params_3.mapValues(lambda (a,b,c,d,e,f,g,h):(h,correlation(h,a,b,c,d,e)))
		simmil_metrics= vectorial_params_3.mapValues(lambda (a,b,c,d,e,f,g,h):(h,cosineSimilarity(a,math.sqrt(d),math.sqrt(e))))
		#Me cojo correlaciones superiores al 50%
		crit = simmil_metrics.filter(lambda ((a,b),(x,y)):y>0.5)
		#desdoblo cada par que tiene una media superior al 50%
		crit_long = crit.flatMap(lambda((a,b),(x,y)):((a,(b,y)),(b,(a,y))))
		#genero el json con todo el CRM y lo persisto
		#se genera un archivo con todo el  CRM y un threshold de ssimilaridad mayor del 50%
			
		if True:
			crit_long_grouped = crit_long.groupByKey().map(lambda (a_lg,b_lg):{"user": a_lg,"similars":[x for x in b_lg],"len":len(b_lg)})
			total_users = crit_long_grouped.count()
			to_file= crit_long_grouped.toDF()
			to_file_J=to_file.toJSON()
			to_file_J.saveAsTextFile("Users_Data2.json")
		
		return total_users




#    def segment_similarity(segment, similarity):