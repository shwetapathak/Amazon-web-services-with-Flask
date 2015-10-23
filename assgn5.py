# Assignment 5 - Introduction to Amazon Web Services (and Web Interface)

# Name : Shweta Pathak
# UTA ID : 1001154572
# Net Id : ssp4572

# import statements

import boto
import csv
import time
import sys
import urllib2
import boto.dynamodb
from boto.s3.key import Key
from boto.s3.connection import S3Connection
from boto.s3.connection import Location
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER

# Upload the file to amazon S3
def put():

    # Access keys 
    AWS_ACCESS_KEY_ID='AKIAJGQE6BZY4X7LYAYA'
    AWS_SECRET_ACCESS_KEY='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'

    # Establish connection with Amazon S3
    conn = S3Connection(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,validate_certs=False,is_secure=False)

    bucket_name = conn.create_bucket('shwetabucket91')
    k = Key(bucket_name)
    k.key = raw_input("Enter the file name to upload to S3: ")
    start_time = time.clock()
    k.set_contents_from_filename(k.key)
    end_time = time.clock()
    total_time = end_time-start_time
    print(total_time) 
      
# Inserting data into the Relational database service 
def createTable():
    
    # Access keys 
    AWS_ACCESS_KEY_ID='AKIAJGQE6BZY4X7LYAYA'
    AWS_SECRET_ACCESS_KEY='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'
    connectDynamo = boto.dynamodb.connect_to_region('us-west-2',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    # Creating a table in Dynamo DB
    #Schema creation
    print "\nCreating a table ........"
       
    inpatient_table_schema = connectDynamo.create_schema(hash_key_name='Id',hash_key_proto_value=str)
    inpatient_table = connectDynamo.create_table(name = 'Inpatient_data',schema = inpatient_table_schema,read_units=10,write_units=50)
    
    #Listing the tables created
    print "\nList of tables created :"
    tables = connectDynamo.list_tables()
    print (tables[0:])
    
# Inserting data into the Dynamo DB table
def insert_data():
    
    # Access keys 
    AWS_ACCESS_KEY_ID='AKIAJGQE6BZY4X7LYAYA'
    AWS_SECRET_ACCESS_KEY='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'

    # Establish connection with Amazon S3
    conn = S3Connection(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,validate_certs=False,is_secure=False)

    bucket_name = conn.get_bucket('shwetabucket91')
    k = Key(bucket_name)
    
    # Reading the file from amazon S3 bucket
    url = 'https://s3.amazonaws.com/shwetabucket91/data1.csv'
    response = urllib2.urlopen(url)
    csv_data = csv.reader(response)
    

    # Establish a connection with dynamodb2
    
    connectDynamo = boto.dynamodb.connect_to_region('us-west-2',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    print "\nInserting to data into table .... "
    table = connectDynamo.get_table('Inpatient_data')
    count = 0
    hashKey_count = 1
    
    start_time = time.clock()
    for row in csv_data:
        count += 1
        hashKey_count += 1
        key = 'Id'+ str(hashKey_count)
        item_data = {'DRG_Definition': row[0],
                    'Provider_Id': row[1],
                    'Provider_Name': row[2],
                    'Address': row[3],
                    'City': row[4],
                    'State': row[5],
                    'Zip': row[6],
                    'Region': row[7],
                    'Total_discharge': row[8],
                    'Average_Covered_Charges': row[9],
                    'Average_Total_Payments': row[10],
                    'Average_Medicare_Payments': row[11]}
        item = table.new_item(hash_key = key ,attrs = item_data)
        item.put()
        print count
    
    end_time = time.clock()
    total_time = end_time - start_time
    print "\n Total time taken to insert data into Dynamo DB :"
    print(total_time)
            
   
# Listing all the buckets available
def list_tables():
    # Access keys 
    AWS_ACCESS_KEY_ID='AKIAJGQE6BZY4X7LYAYA'
    AWS_SECRET_ACCESS_KEY='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'
    connectDynamo = boto.dynamodb.connect_to_region('us-west-2',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    #Listing the tables created
    print "\nList of tables created :"
    tables = connectDynamo.list_tables()
    print (tables[0:])

# Query data stored in Dynamo DB

def main():
  
  options_toselect = {1: put, 2: createTable, 3:insert_data,4:list_tables}
  while(True):
     
      print "\n1. Upload file on Amazon Cloud S3. \n"
      print "2. Create table into Amazon Dynamo DB \n"
      print "3. Insert data into Amazon Dynamo DB table \n"
      print "4. List tables created on Dynamo DB. \n"
      print "5. Exit \n"
     
      option = raw_input("Select one option : ")
      if option =="1":
          options_toselect[1]()
      elif option =="2":
          options_toselect[2]()
      elif option =="3":
          options_toselect[3]()
      elif option =="4":
          options_toselect[4]()
      elif option =="5":
          sys.exit(0)
      else:
          print "Please select a valid choice !!!\n"


if __name__ == '__main__':
  main()

