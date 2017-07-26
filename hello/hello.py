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

@app.route('/')
def start():
    print("begin main")
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
    now_time = int(time.time())
    table = []
    with open("/Users/kchinnappan/PycharmProjects/hello/fake_udp_data.json", 'r') as f:
        for line in f:
            table.append(json.loads(line))
    for row in table:
        print(row)
        print(row['header']['role'])
        curr_time = row['header']['time']
        fromm = row['header']['from']
        role = row['header']['role']
        if role == "resrc":
            print("role == resrc")
            cpus = row['payload']['cpus']
            mems = row['payload']['mems']
            disk = row['payload']['disk']
            collection.insert({"now": now_time, "from": fromm,"role": role, "time": curr_time, "payload": {"cpus": cpus, "mems": mems, "disk": disk}})
        elif role == "prepr":
            print("role == prepr")
            node = row['payload']['node']
            ints = row['payload']['ints']
            pcrd = row['payload']['pcrd']
            mods = row['payload']['mods']
            rcvd = row['payload']['rcvd']
            prcd = row['payload']['prcd']
            invl = row['payload']['invl']
            incm = row['payload']['incm']
            totl = row['payload']['totl']
            genr = row['payload']['genr']
            modg = row['payload']['modg']
            publ = row['payload']['publ']
            collection.insert({"now": now_time, "from": fromm,"role": role, "time": curr_time,"payload": {"node": node, "ints": ints, "pcrd": pcrd, "mods": mods, "rcvd": rcvd,
                                       "prcd": prcd, "invl": invl, "incm": incm, "totl": totl,"genr": genr, "modg": modg, "publ": publ}})
        elif role == "kafka" or role == "druid":
            if role == "kafka":
                print("role == kafka")
            elif role == "druid":
                print("role == druid")
            flow = row['payload']['flow']
            swit = row['payload']['swit']
            anml = row['payload']['anml']
            collection.insert({"now": now_time, "from": fromm,
                       "role": role, "time": curr_time, "payload": {"flow": flow, "swit": swit, "anml": anml}})
        elif role == "spark":
            print("role == spark")
            inpt = row['payload']['inpt']
            schd = row['payload']['schd']
            prcs = row['payload']['prcs']
            totl = row['payload']['totl']
            collection.insert({"now": now_time, "from": fromm,
                       "role": role, "time": curr_time,
                       "payload": {"inpt": inpt, "schd": schd, "prcs": prcs, "totl": totl}})

    return jsonify(row)
    # response_from = request.json.get('from')
    # response_to = request.json.get('to')
    # response_sample = request.json.get('sample')
    #
    # d1 = datetime.strptime(response_from, "%Y%m%d%H%M%S")
    # time11 = int(time.mktime(d1.timetuple()))
    # print("time11: ", time11)
    # d2 = datetime.strptime(response_to, "%Y%m%d%H%M%S")
    # time22 = int(time.mktime(d2.timetuple()))
    # print("time22: ", time22)
    # #print(response)
    # if request.method == 'POST':
    #     L = []
    #     for x in collection.find({"time": {"$gte": time11,
    #                                        "$lte": time22}},
    #                              {"payload": 1, "_id": 0}).limit(response_sample):
    #         L.append(x['payload'])
    #     print(L)
    #     #print(response_from)
    #     #print(response_to)
    #     #print(response_sample)
    #     return dumps(L)
    # elif request.method == 'GET':
    #     return "Ok, this is a GET request"

if __name__ == '__main__':
    app.run()
