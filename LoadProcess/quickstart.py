import gspread as gs
import pandas as pd
import boto3
import psycopg2
import psycopg2.extras


#Configuration that we can move to external files
URL_KEY = "1jsHbDNyv92_EbIHK5sLkMcuNidiQpJUkUKQ1KAZeHj4"
ENDPOINT = "REPLACE WITH USER ENDPOINT"
USER = "REPLACE WITH DEV USER NAME"
PORT = 5432
REGION="REPLACE WITH REGION"
DBNAME="Calls"


#Method to read the public google sheet
def readFile():
    dataframe = pd.read_csv('https://docs.google.com/spreadsheets/d/' + 
                   URL_KEY +
                   '/export?gid=0&format=csv',
                  )
    return dataframe

#Method to read data in the local computer
def readData():
    dataframe = pd.read_csv("example.csv")
    return dataframe


#Method to store data
def storeData():


    #gets the credentials from .aws/credentials
    session = boto3.Session(profile_name='dev_user')
    client = session.client('rds')

    #method generate token to authenticate
    token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)
    
    #table name to store raw data
    table = "CallsRaw"

    try:
    
        #create connection
        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=token, sslrootcert="SSLCERTIFICATE")
       
        #read dataframe
        df = readFile()
        
        #remove unnused column
        df = df.loc[:, df.columns!='Unnamed: 0']
        

        if len(df) > 0:
            cur = conn.cursor()
            df_columns = list(df)
            # create (col1,col2,...)
            columns = ",".join(df_columns)

            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 

            #create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO raw.{} ({}) {}".format(table,columns,values)
            
            #query to store data
            createTable = "CREATE TABLE IF NOT EXISTS raw.{} (id serial PRIMARY KEY, start varchar, duration varchar, channels varchar);".format(table)
            
            #create table in database
            cur.execute(createTable)
            
            #store dataframe in db
            psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
            conn.commit()
            cur.close()
        

    except Exception as e:
        print("Database connection failed due to {}".format(e))  
        cursor.close()
        return 1
    




def main():
    storeData()

if __name__ == "__main__":
    main()

