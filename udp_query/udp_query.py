# udp_query.pymongo
# 25/07/2017
# example: curl  -d '{"from": "20170726152000", "to":"20170726160000", 
# "sample": 2, "role":"resrc"}' -H "Content-Type: application/json" 
# -X POST http://0.0.0.0:1234

# replace host and port number accordingly
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
import itertools

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
    db = conn.udp_reports
    # create a collection
    collection = db.udp_collection

    #get the request fields from query, get the json field
    response_from = request.json.get('from')
    response_to = request.json.get('to')
    response_sample = request.json.get('sample')
    response_role = request.json.get('role')

    #convert the extracted time(yyyymmddHHMMSS) to integer format
    d1 = datetime.strptime(response_from, "%Y%m%d%H%M%S")
    time11 = int(time.mktime(d1.timetuple()))
    print("time11: ", time11)
    d2 = datetime.strptime(response_to, "%Y%m%d%H%M%S")
    time22 = int(time.mktime(d2.timetuple()))
    print("time22: ", time22)

    print("role== ", response_role)
    #if request method == POST, perform the queries
    if request.method == 'POST':
        if response_role == "kafka":
            print("in kafka")
# pipelime = match by role and time, unwind(decostruct) array, limit by sample# and group
            pipeline = [{"$match":{"role":"resrc","time": {"$gte": time11,
                                           "$lte": time22}}},{"$unwind":"$payload.swit"},{"$unwind":"$payload.flow"},{"$unwind":"$payload.anml"},
        {"$limit":response_sample},{"$group":{"_id":0, "swit":{"$push":"$payload.swit"}, "flow":{"$push":"$payload.flow"}, "anml":{"$push":"$payload.anml"}}},{"$project":{"_id":0}}]
            cursor = collection.aggregate(pipeline)
            new_cursor = list(cursor)
            print(new_cursor)
            return dumps(new_cursor)
        elif response_role == "resrc":
            print("in resrc")
            pipeline = [{"$match":{"role":"resrc","time": {"$gte": time11,
                                           "$lte": time22}}},{"$unwind":"$payload.cpus"},{"$unwind":"$payload.mems"},{"$unwind":"$payload.disk"},
        {"$limit":response_sample},{"$group":{"_id":0, "cpus":{"$push":"$payload.cpus"}, "mems":{"$push":"$payload.mems"}, "disk":{"$push":"$payload.disk"}}},{"$project":{"_id":0}}]
            cursor = collection.aggregate(pipeline)
            new_cursor = list(cursor)
            print(new_cursor)
            return dumps(new_cursor)
        elif response_role == "druid":
            print("in druid")
            pipeline = [{"$match":{"role":"druid","time": {"$gte": time11,
                                           "$lte": time22}}},{"$unwind":"$payload.swit"},{"$unwind":"$payload.flow"},{"$unwind":"$payload.anml"},
        {"$limit":response_sample},{"$group":{"_id":0, "swit":{"$push":"$payload.swit"}, "flow":{"$push":"$payload.flow"}, "anml":{"$push":"$payload.anml"}}},{"$project":{"_id":0}}]
            cursor = collection.aggregate(pipeline)
            new_cursor = list(cursor)
            print(new_cursor)
            return dumps(new_cursor)
        elif response_role == "spark":
            print("in spark")
            pipeline = [{"$match":{"role":"spark","time": {"$gte": time11,
                                           "$lte": time22}}},{"$unwind":"$payload.inpt"},{"$unwind":"$payload.schd"},{"$unwind":"$payload.prcs"},{"$unwind":"$payload.totl"},
        {"$limit":response_sample},{"$group":{"_id":0, "inpt":{"$push":"$payload.inpt"}, "schd":{"$push":"$payload.schd"}, "prcs":{"$push":"$payload.prcs"}, "totl":{"$push":"$payload.totl"}}},{"$project":{"_id":0}}]
            cursor = collection.aggregate(pipeline)
            new_cursor = list(cursor)
            print(new_cursor)
            return dumps(new_cursor)
        elif response_role == "prepr":
            print("in prepr")
            pipeline = [{"$match":{"role":"prepr","time": {"$gte": time11,
                                           "$lte": time22}}},{"$unwind":"$payload.node"},{"$unwind":"$payload.ints"},{"$unwind":"$payload.pcrd"},{"$unwind":"$payload.mods"},{"$unwind":"$payload.rcvd"},{"$unwind":"$payload.prcd"},{"$unwind":"$payload.invl"},{"$unwind":"$payload.incm"},{"$unwind":"$payload.totl"},{"$unwind":"$payload.genr"},{"$unwind":"$payload.modg"},{"$unwind":"$payload.publ"},
        {"$limit":response_sample},{"$group":{"_id":0, "node":{"$push":"$payload.node"}, "ints":{"$push":"$payload.ints"}, "pcrd":{"$push":"$payload.pcrd"},"mods":{"$push":"$payload.mods"},"rcvd":{"$push":"$payload.rcvd"},"invl":{"$push":"$payload.invl"},"incm":{"$push":"$payload.incm"},"totl":{"$push":"$payload.totl"},"genr":{"$push":"$payload.genr"},"modg":{"$push":"$payload.modg"},"publ":{"$push":"$payload.publ"}}},{"$project":{"_id":0}}]
            cursor = collection.aggregate(pipeline)
            new_cursor = list(cursor)
            print(new_cursor)
            return dumps(new_cursor)
    elif request.method == 'GET':
        return "Ok, this is a GET request"

if __name__ == '__main__':
    app.run(host='10.201.208.90', port=5000) #change for appropriate host and port,
                                       #currently running on http://0.0.0.0:1234
