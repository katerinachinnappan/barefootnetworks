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
import socket

def begin():
    UDP_IP = "10.201.208.90"
    UDP_PORT = 8086

    sock = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP
    sock.bind((UDP_IP, UDP_PORT))

    print("begin main")
    print("begin mongo connection")
    try:
        conn = pymongo.MongoClient()
        print ("Connected successfully!!!")
    except pymongo.errors.ConnectionFailure as e:
        print ("Could not connect to MongoDB: %s") % e
    conn
#create a database
    db = conn.udp_reports
#create a collection
    collection = db.udp_collection
    now_time = int(time.time())
    table = []

    while True:
        data, addr = sock.recvfrom(1024) # buffer size is 1024 bytes
        table.append(json.loads(data))
        for row in table:
            curr_time = row['header']['time']
            fromm = row['header']['from']
            role = row['header']['role']
            if role == "resrc":
                print("role == resrc")
                cpus = row['payload']['cpus']
                mems = row['payload']['mems']
                disk = row['payload']['disk']
                collection.insert({"now": now_time, "from": fromm, "role": role, "time": curr_time, "payload": {"cpus": cpus, "mems": mems, "disk": disk}})
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
                collection.insert({"now": now_time, "from": fromm,"role": role, "time": curr_time, "payload":{"node": node, "ints": ints, "pcrd": pcrd, "mods": mods, "rcvd": rcvd,"prcd": prcd, "invl": invl, "incm": incm, "totl": totl, "genr": genr, "modg": modg, "publ": publ}})
            elif role == "kafka" or role == "druid":
                if role == "kafka":
                    print("role == kafka")
                elif role == "druid":
                    print("role == druid")
                flow = row['payload']['flow']
                swit = row['payload']['swit']
                anml = row['payload']['anml']
                collection.insert({"now": now_time, "from": fromm, "role": role, "time": curr_time, "payload":{"flow": flow, "swit": swit, "anml": anml}})
            elif role == "spark":
                print("role == spark")
                inpt = row['payload']['inpt']
                schd = row['payload']['schd']
                prcs = row['payload']['prcs']
                totl = row['payload']['totl']
                collection.insert({"now": now_time, "from": fromm,"role": role, "time": curr_time, "payload":{"inpt": inpt, "schd": schd, "prcs": prcs, "totl": totl}})
    
#main start
begin()
#main end
