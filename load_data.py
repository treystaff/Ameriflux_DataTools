#load_data contains the data loading functions.
# Ameriflux data are inserted into an sqlite database.
import sqlite3, csv, tempfile

def db_connect(db):
	#Returns a database connection and cursor.
	# (maybe put this in a separate tools file?)
	conn = sqlite3.connect(db)
	cursor = conn.cursor()
	return conn, cursor

def create_site(db,site,codename):
	#Tool to add new site to the sites table. Returns ID.
	conn,cursor = db_connect(db)
	cursor.execute('SELECT ID from sites WHERE site_name = ? AND code_name = ?;',[site,codename])
	site_id = cursor.fetchone()
	if site_id:
		return 0,'THE SITE ALREADY EXISTS.'
	else:
		#Insert the record
		cursor.execute('INSERT INTO sites (site_name, code_name) VALUES (?,?);',[site,codename])
		conn.commit()
		#Get the new record's ID and return it (Note sqlite does not seem to have a 'RETURNING' clause). 
		cursor.execute('SELECT ID from sites WHERE site_name = ? AND code_name = ?;',[site,codename])
		site_id = cursor.fetchone()
		return 1,site_id
		
	
def import_L2_data(db,data_path,codename):
	#This function currently only inserts L2 data.
	
    #Create a connecton to the database.
    conn,cursor = db_connect(db)

    #Determine the id of the given codename
    cursor.execute('SELECT id FROM sites WHERE code_name = ?', codename)
    site_id = cursor.fetchone()
    if not site_id:
        return 0,'THE GIVEN site DOES NOT APPEAR TO BE IN THE DATABASE.'

    #Open the ameriflux data file.
    with open(data_path,'rb') as file:
		reader = csv.reader(file,delimiter=',')
		#skip the headerlines
		for skip in range(20):
			next(reader,None)
			
		#Read each remaining line from the ameriflux file;
		data = []
		for row in reader:
			data.append(tuple(row.insert(0,site_id)))
			
		#Insert the data into the database. Wrap in a transaction. 
		cursor.execute('BEGIN')
		cursor.executemany('INSERT INTO L2 VALUES (' + '?,'*41 + '?' + ');',data)
		cursor.execute('COMMIT')
		conn.commit()