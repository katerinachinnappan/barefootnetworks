from flask import Flask
from flask import request
import json;
import time;
import pymongo;
from pymongo import MongoClient;
from pprint import pprint;
from datetime import datetime;
from flask import jsonify
from bson.json_util import dumps

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def query():
    print("begin mongo connection")
    try:
        conn = pymongo.MongoClient()
        print("Connected successfully!!!")
    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB: %s") % e
    conn
    # create a database
    db = conn.udp_test_db
    # create a collection
    collection = db.udp_collection

    response_from = request.json.get('from')
    response_to = request.json.get('to')
    response_sample = request.json.get('sample')

    d1 = datetime.strptime(response_from, "%Y%m%d%H%M%S")
    time11 = int(time.mktime(d1.timetuple()))
    print("time11: ", time11)
    d2 = datetime.strptime(response_to, "%Y%m%d%H%M%S")
    time22 = int(time.mktime(d2.timetuple()))
    print("time22: ", time22)
    #print(response)
    if request.method == 'POST':
        L = []
        for x in collection.find({"time": {"$gte": time11,"$lte": time22}},
                                 {"payload": 1, "_id": 0}).limit(response_sample):
            L.append(x['payload'])
        print(L)
        #print(response_from)
        #print(response_to)
        #print(response_sample)
        return dumps(L)
    elif request.method == 'GET':
        return "Ok, this is a GET request"

if __name__ == '__main__':
    app.run(host='10.201.208.90', port=8086)
