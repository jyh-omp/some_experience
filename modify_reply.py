#!/usr/bin/env python
#coding=GBK


import MySQLdb as mysqldb
import MySQLdb.cursors
import pyodbc
import pymssql
import time
import datetime
import json
import urllib2
import gc
import sys
reload(sys)
sys.setdefaultencoding( "GBK" )


class topic_info:
	def __init__(self):
		self.replyCount = -1
		self.lastReplyDate = ""

def read_reply_date(topic_dict):
	conn = pyodbc.connect('DRIVER={MSSQL};SERVER=DB-SEARCH-A;port=1433;DATABASE=AutoSearch;UID=Search_Reader;PWD=5BC13EA6-C6E6-40ED-85AC-8BED86750E06;TDS_Version=7.0;')
	cursor = conn.cursor()
	#两年days=730
	tm = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")						  

	sql = "select [topicId],[tlastPostDate] from [AutoSearch].[dbo].[AutoTopic] where tlastPostDate > '"+ tm + "'"
	print sql
	cursor.execute(sql)
	
	sum = 0
	while 1:
		try:
			rows = cursor.fetchmany(1000000)
		except Exception,ex:
			print Exception,":",ex
			print "repeat connect"
			conn.close()
			cursor.close()
			conn_cnt = 0
			while conn_cnt <= 2:
				try:
					conn = pyodbc.connect('DRIVER={MSSQL};SERVER=DB-SEARCH-A;port=1433;DATABASE=AutoSearch;UID=Search_Reader;PWD=5BC13EA6-C6E6-40ED-85AC-8BED86750E06;TDS_Version=7.0;')
					break
				except Exception,ex:
					print Exception,":",ex
					conn_cnt += 1
					time.sleep(1)
			cursor = conn.cursor()
			cursor.execute(sql)
			rows = cursor.fetchmany(100000)
		if len(rows) == 0:
			break
		sum += len(rows)
		print "processed line: ",sum
		for row in rows:
			topicInfo = topic_info()
			topic_id  = row[len(row) - 2]
			topicInfo.lastReplyDate = row[len(row) - 1]
			topic_dict[topic_id] = topicInfo	
		gc.collect()
	
	cursor.close()
	conn.close()

def read_reply_cnt(topic_dict):
	conn = pyodbc.connect('DRIVER={MSSQL};SERVER=DB-SEARCH-A;port=1433;DATABASE=AutoSearch;UID=Search_Reader;PWD=5BC13EA6-C6E6-40ED-85AC-8BED86750E06;TDS_Version=7.0;')
	cursor = conn.cursor()

	tm = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

	t_count = 0
	while t_count <= len(topic_dict):
		sql = '''
			  select
			  		top 1000 AT.[topicId], AT.[tlastPostDate], TC.[Replys] 
			  from
			  		[AutoSearch].[dbo].[AutoTopic] AT  
			  left join
			  		[AutoSearch].[dbo].[TopicClicks] TC
			  on
			  		AT.topicId = TC.TopicId
			  where 
			  		AT.tlastPostDate > '%s'  and AT.topicId > %d
			  order by
			  		AT.topicId

			  ''' % (tm, max_id)

		print 'sql', sql
		cursor.execute(sql) 
		print 'cursor'
		try:
			rows = cursor.fetchall()
			print 'fetch'
		except Exception,ex:
			print Exception,":",ex
			print "repeat connect"
			conn.close()
			cursor.close()
			conn_cnt = 0
			while conn_cnt <= 2:
				try:
					conn = pyodbc.connect('DRIVER={MSSQL};SERVER=DB-SEARCH-A;port=1433;DATABASE=AutoSearch;UID=Search_Reader;PWD=5BC13EA6-C6E6-40ED-85AC-8BED86750E06;TDS_Version=7.0;')
					break
				except Exception,ex:
					print Exception,":",ex
					conn_cnt += 1
					time.sleep(1)
		
			cursor = conn.cursor()
			cursor.execute(sql)
			rows = cursor.fetchall()

		if rows is not None:
			for i in xrange(len(rows)):
				topic_dict[rows[i][0]].replyCount = rows[i][2]

		max_id = rows[-1][0]
		t_count += 1000

	'''
	total_count = len(sorted_topic_dict)
	max_id = 0
	for t_count in xrange(total_count):
		if t_count > 10000:
			break
		
		if (t_count >= 1000 and t_count % 1000 == 0) or t_count == total_count:
			print '1000 start'
			sql = "select top 1000 [TopicId],[Replys] from [AutoSearch].[dbo].[TopicClicks] where TopicId > " + str(max_id) + " order by TopicId" 
			print 'sql', sql
			cursor.execute(sql) 
			print 'cursor'
			try:
				rows = cursor.fetchall()
				print 'fetch'
			except Exception,ex:
				print Exception,":",ex
				print "repeat connect"
				conn.close()
				cursor.close()
				conn_cnt = 0
				while conn_cnt <= 2:
					try:
						conn = pyodbc.connect('DRIVER={MSSQL};SERVER=DB-SEARCH-A;port=1433;DATABASE=AutoSearch;UID=Search_Reader;PWD=5BC13EA6-C6E6-40ED-85AC-8BED86750E06;TDS_Version=7.0;')
						break
					except Exception,ex:
						print Exception,":",ex
						conn_cnt += 1
						time.sleep(1)
			
				cursor = conn.cursor()
				cursor.execute(sql)
				rows = cursor.fetchall()

			if rows is not None:
				for i in xrange(len(rows)):
					topic_dict[rows[i][0]].replyCount = rows[i][1]

			max_id = rows[-1][0]
	'''

	cursor.close()
	conn.close()

def datetime_toString(dt):  
	return dt.strftime("%Y-%m-%d %H:%M:%S") 

def modify(topic_dict):
	for id in topic_dict:
		print id,topic_dict[id].replyCount
		if topic_dict[id].replyCount >= 0:
			row = json.dumps({'replyCount':topic_dict[id].replyCount, 'lastReplyDate':datetime_toString(topic_dict[id].lastReplyDate)})
		else:
			row = json.dumps({'lastReplyDate':datetime_toString(topic_dict[id].lastReplyDate)})	
		data = {}
		data['docid'] = id
		data['data'] = row
		data['operation'] = 1
		jdata = json.dumps(data)

		req = urllib2.Request(url='http://192.168.199.5:9192/topic', data=jdata)
		f = urllib2.urlopen(req)
		rep = f.read()
		jrep = json.loads(rep)
		if jrep['returncode'] != 0:
			print "Error occured ", jrep['message']

if __name__ == "__main__":
	topic_dict = {}
	start_time = time.time()
	read_reply_date(topic_dict)
	end_time = time.time()
	print "read replyDate end,cost time:",(end_time - start_time)

	start_time = time.time()
	read_reply_cnt(topic_dict)
	end_time = time.time()
	print "read replyCnt end,cost time:",(end_time - start_time)

	start_time = time.time()
	#modify(topic_dict)
	end_time = time.time()
	print "modify end,cost time:",(end_time - start_time)

	print "run Done."
