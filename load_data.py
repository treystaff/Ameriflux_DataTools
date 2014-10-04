#load_data contains the data loading functions.
# Ameriflux data are inserted into an sqlite database.
import sqlite3, csv, tempfile

def db_connect(db):
	#Returns a database connectiton and cursor.
	# (maybe put this in a separate tools file?)
	conn = sqlite3.connect(db)
	cursor = conn.cursor()
	return conn, cursor

def create_site(db,site,codename):
	#Tool to add new site to the sites table. Returns ID.
	conn,cursor = db_connect(db)
	cursor.execute('''SELECT ID from sites WHERE site_name = ?
		AND code_name = ?;''',[site,codename])
	site_id = cursor.fetchone()
	if site_id:
		return 0,'THE SITE ALREADY EXISTS.'
	else:
		#Insert the record
		cursor.execute('''INSERT INTO sites (site_name, code_name)
			VALUES (?,?);''',[site,codename])
		conn.commit()
		#Get the new record's ID and return it (Note sqlite does not seem to have
		# a 'RETURNING' clause).
		cursor.execute('''SELECT ID from sites WHERE site_name = ?
			AND code_name = ?;''',[site,codename])
		site_id = cursor.fetchone()
		return 1,site_id


def import_L2_data(db,data_path,codename):
	#Inserts L2 data into an sqlite3 db.

	#Create a connecton to the database.
	conn,cursor = db_connect(db)

	#Determine the id of the given codename
	cursor.execute('SELECT id FROM sites WHERE code_name = ?', [codename])
	site_id = cursor.fetchone()
	if not site_id:
	    return 0,'THE GIVEN site DOES NOT APPEAR TO BE IN THE DATABASE.'

	#Open the ameriflux data file.
	with open(data_path,'rb') as file:
		reader = csv.reader(file,delimiter=',')

		#skip the headerlines (maybe get info from later)
		for skip in range(20):
			next(reader,None)

		#Read each remaining line from the ameriflux file;
		for row in reader:
			#Add entries for site_id and the PK (NULL will be filled w/ rowid)
			row.insert(0,site_id[0])
			row.insert(0,None)
			#47 total cols of data to insert for each record
			cursor.execute('INSERT INTO L2 VALUES (' + '?,'*46 + '?' + ');',row)

		conn.commit()

def import_L4_h_data(db,data_path,year,codename):
	#Inserts L4 data into an sqlite3 db
	# requires year for insert.
	#Create a connecton to the database.
	conn,cursor = db_connect(db)

	#Determine the id of the given codename
	cursor.execute('SELECT id FROM sites WHERE code_name = ?', [codename])
	site_id = cursor.fetchone()
	if not site_id:
		return 0,'THE GIVEN site DOES NOT APPEAR TO BE IN THE DATABASE.'

	#Open the ameriflux data file.
	with open(data_path,'rb') as file:
		reader = csv.reader(file,delimiter=',')

		#skip the headerline
		next(reader,None)

		#Read each remaining line from the ameriflux file;
		for row in reader:
			#Add entries for year, site_id and the PK (NULL will be filled w/ rowid)
			row.insert(0,year)
			row.insert(0,site_id[0])
			row.insert(0,None)
			#37 total cols of data to insert for each record
			cursor.execute('INSERT INTO L4_h VALUES (' + '?,'*36 + '?' + ');',row)

	conn.commit()
