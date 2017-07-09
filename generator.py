# tested with python2.7 and 3.4
#!/usr/bin/env python
# -*- coding: utf-8 -*- 
from pyspark import Row
from pyspark.sql.functions import col,udf, unix_timestamp, StringType
from pyspark.sql.types import DateType
import pandas as pd
from pyspark import SparkContext 
from pyspark.sql import SQLContext
from pyspark import SparkContext 

import sys

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




class segment_builder:
	"""A movie recommendation engine
	"""
	
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
	