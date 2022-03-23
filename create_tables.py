#!pip install psycopg2
import psycopg2

conn_to_db = " dbname='zakupki' user='postgres' host='localhost' password='password' port='5432'"


def connect_to_db():
	conn = psycopg2.connect(conn_to_db)
	cur = conn.cursor()
	return conn, cur


def create_tables():
	""" create tables in the PostgreSQL database"""
	commands = ("""
		CREATE TABLE regions (
			id SERIAL PRIMARY KEY,
			region VARCHAR(100) UNIQUE
	)""",
	"""
		CREATE TABLE procurers (
			reg_num BIGINT PRIMARY KEY,
			INN BIGINT,
			name VARCHAR(1000)
			
	)""",
	"""
		CREATE TABLE participants (
			INN BIGINT PRIMARY KEY,
			name VARCHAR(1000)
	)""",
	"""
		CREATE TABLE finance_sources (
			id SERIAL PRIMARY KEY,
			source VARCHAR(1000) UNIQUE
	)""",
	"""
		CREATE TABLE auctions (
			id SERIAL PRIMARY KEY,
			purchase_number VARCHAR(50) UNIQUE,
			region_id SMALLINT REFERENCES regions (id) ON DELETE RESTRICT ON UPDATE CASCADE,
			procurer_reg_num BIGINT REFERENCES procurers (reg_num) ON DELETE RESTRICT ON UPDATE CASCADE,
			max_price DECIMAL(22, 2),
			currency VARCHAR(3),
			start_date TIMESTAMP,
			end_date TIMESTAMP,
			prolong_date TIMESTAMP,
			n_commission_members SMALLINT,
			finance_source_id INT REFERENCES finance_sources (id) ON DELETE RESTRICT ON UPDATE CASCADE,
			delivery_term VARCHAR(200)
			electronic BOOLEAN DEFAULT False
	)""",  # general info about auctions; if prolong_date is NULL then auction was not prolonged
	"""
		CREATE TABLE bids (
			auction_id INT REFERENCES auctions (id) ON DELETE CASCADE ON UPDATE CASCADE,
			participant_INN BIGINT REFERENCES participants (INN) ON DELETE RESTRICT ON UPDATE CASCADE,
			price DECIMAL(22, 2),
			date TIMESTAMP,
			is_approved BOOLEAN DEFAULT TRUE,
			is_after_prolong BOOLEAN DEFAULT FALSE,
			PRIMARY KEY (auction_id, participant_INN)
	)""",
	"""
		CREATE TABLE purchase_objects (
			id SERIAL PRIMARY KEY,
			code_1 SMALLINT,
			code_2 SMALLINT,
			code_3 SMALLINT,
			code_4 SMALLINT,
			if_OKPD2 BOOLEAN,
			name VARCHAR(1000) UNIQUE
	)""",  # list of objects that can be purchased through procurement auctions in general (and their codes)
	"""
		CREATE TABLE auction_purchase_objects (
			auction_id INT NOT NULL,
			purchase_object_id INT NOT NULL,
			PRIMARY KEY (auction_id, purchase_object_id),
			
			FOREIGN KEY (auction_id)
				REFERENCES auctions (id) ON DELETE CASCADE ON UPDATE CASCADE,
			FOREIGN KEY (purchase_object_id)
				REFERENCES purchase_objects (id) ON DELETE RESTRICT ON UPDATE CASCADE
	)""",  # info on which objects are bought in specific auctions
	"""
		CREATE TABLE correspondences (
			id SERIAL PRIMARY KEY,
			name varchar(1000) UNIQUE,
			code varchar(20),
			short_name varchar(20)
	)""",  # list of requirements for participants that can be found in auctions in general (and their codes)
	"""
		CREATE TABLE auction_correspondences (
			auction_id INT NOT NULL,
			correspondence_id INT NOT NULL,
			PRIMARY KEY (auction_id, correspondence_id),
			FOREIGN KEY (auction_id)
				REFERENCES auctions (id) ON DELETE CASCADE ON UPDATE CASCADE,
			FOREIGN KEY (correspondence_id)
				REFERENCES correspondences (id) ON DELETE RESTRICT ON UPDATE CASCADE
	)""")

	conn = None
	try:
		conn, cur = connect_to_db()
		for command in commands:
			cur.execute(command)
		# close communication with the PostgreSQL database server
		cur.close()
		# commit the changes
		conn.commit()
	except Exception as error:
		raise error
	finally:
		if conn is not None:
			conn.close()


create_tables()
