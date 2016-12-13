import sys
import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def connectToDatabase(connection):
	#print the connection string we will use to connection
	print "Connecting to postgres:\n%s" % (connection)
		
	#get a connection, if a connect cannot be made an exeception will beraised here
	conn = psycopg2.connect(connection)
		
	#set autocommit which allows to create a new database
	conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
		
	#conn.cursor will return a cursor object, you can use this cursor to perform queries
	
	return conn
	
def createTable(cursor):
	
	try: 
		cursor.execute('CREATE TABLE lbl_dataset (\
		timestamp double precision,\
		duration text,\
		protocol text,\
		bsbo text,\
		bsbr text,\
		localhost integer,\
		remotehost text,\
		state text,\
		flag text\
		);')
		print "table has been created!\n"
	except:
		print "table lbl_dataset already exists!\n"
		
def insertData(cursor):
	
	#File with data
	txtFile = "C:\Users\PLBAPER\Desktop\szkola\lbl-conn-7.red"
	#table to store the data
	columns = list();
	
	i = 0;
	
	try:
		file = open(txtFile, 'r')
		
		for line in file:
		
			columns = line.split()
	
			if len(columns) == 8:
				columns.append("UNK")
				
			print columns
			
			cursor.execute('INSERT INTO lbl_dataset VALUES (' + \
							columns[0] + \
							', \'' + columns[1] + '\'' + \
							', \'' + columns[2] + '\'' + \
							', \'' + columns[3] + '\'' + \
							', \'' + columns[4] + '\'' + \
							', ' + columns[5] + \
							', \'' + columns[6] + '\'' + \
							', \'' + columns[7] + '\'' + \
							', \'' + columns[8] + '\'' + \
							')');
			
			i = i+1;
			
			if i==10:
				break;
	
	finally:
		file.close()
	
def main():

	#Defining connection string
	
	try:
		conn_string = "host='localhost' dbname='lbl_dataset' user='postgres' password='1234'"
		
		conn = connectToDatabase(conn_string)
		
		cursor = conn.cursor()
		print "connected!\n"
		
	except:
		dbname='lbl_dataset'
		
		print "database does not exist!\n"
		print "creating database " + dbname + "\n"
		
		conn_string = "host='localhost' user='postgres' password='1234'"
		
		conn = connectToDatabase(conn_string)
		
		cursor = conn.cursor()
		print "connected!\n"
		
		cursor.execute('CREATE DATABASE ' + dbname + ';')
		print "database has been created!\n"
	
	#Creating Table
	createTable(cursor)
	
	insertData(cursor)
	
	cursor.close()
	conn.close()
	
	
if __name__=="__main__":
	main()