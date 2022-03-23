import psycopg2
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine
from time import sleep

conn_to_db = "host='localhost' dbname='zakupki' user='postgres' password='password'"


def connect_to_db():
	conn = psycopg2.connect(conn_to_db)
	cur = conn.cursor()
	return conn, cur


def one_row_request(request, param, if_commit=False):
	"""
	request - string with valid SQL request
	param - list of used params
	if_commit - set True if changes in database are being made
	"""
	conn = None
	try:
		conn, cur = connect_to_db()
		cur.execute(request, param)
		row = cur.fetchone()
		if if_commit:
			conn.commit()
		cur.close()
	except (Exception, psycopg2.DatabaseError) as error:
		raise error
	finally:
		if conn is not None:
			conn.close()
	return row


def no_return_command(request, param):
	"""
	request - string with valid SQL request
	param = list of used params
	"""
	conn = None
	try:
		conn, cur = connect_to_db()
		cur.execute(request, param)
		conn.commit()
		cur.close()
	except (Exception, psycopg2.DatabaseError) as error:
		raise error
	finally:
		if conn is not None:
			conn.close()


def select_from_multiple_db(query, index_col=None, host='localhost', user='postgres', port=5432, password='password'):
	"""
	extracts and concats data from all the databases into a single DataFrame
	:param password:
	:param port:
	:param user:
	:param host:
	:param query: string, SQL query (SELECT statement)
	:param index_col: Column(s) to set as index(MultiIndex).
	:return: instance of pd.DataFrame
	"""
	dbnames = ['zakupki0' + str(i) for i in range(1, 10)] + ['zakupki10']

	final_df = pd.DataFrame()
	for i in tqdm(range(len(dbnames))):
		# con = "host={host} port={port} dbname={dbname} user={user} password={password}".\
		# 	format(host=host, dbname=dbnames[i], user=user, password=password, port=port)
		# con = psycopg2.connect(con)
		con = create_engine('postgresql://{user}:{password}@{host}:{port}/{dbname}'.
							format(host=host, dbname=dbnames[i], user=user, password=password, port=port))
		try:
			df = pd.read_sql(query, con, index_col=index_col)
			final_df = pd.concat([final_df, df])
		except Exception as e:
			print(e)
		# con.close()
	return final_df
