
from flask import Flask, render_template, request, url_for
from flask import Flask
from flask.ext.dynamo import Dynamo
import boto
import time
from boto.dynamodb.layer1 import Layer1
from boto.dynamodb import condition


# Initialize the Flask application
application = Flask(__name__)

# Define a route for the default URL, which loads the form
@application.route('/')
def form_input():
    
    return render_template('form_submit.html')


@application.route('/GetData/', methods=['POST'])
def GetData():
    start_time = time.clock()
    Provider_Id = request.form['id']
   
    conn = boto.dynamodb.connect_to_region('us-west-2',aws_access_key_id='access_key',aws_secret_access_key='secret_access_key')
    
    table = conn.get_table('Inpatient_data')
    item = table.scan(scan_filter={'Provider_Id': boto.dynamodb.condition.EQ(Provider_Id)})
    items_list=[]
    for i in item:
        items_list.append(i)
        
    end_time=time.clock()
    total = end_time - start_time
    return render_template('form_action.html',time=total,list_item=items_list)
    
# Run the application
if __name__ == '__main__':
  application.run( 
        host="0.0.0.0",
        port=int("80")
  )
