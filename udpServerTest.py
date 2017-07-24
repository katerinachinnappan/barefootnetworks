import json;
import time;
import pymongo;
from pymongo import MongoClient;
from pprint import pprint;
from datetime import datetime;

#####establishing mongodb connection####
def mongo(collection):
    
    collection.create_index("udp_report", unique=True) #this is now the primary key to update any document
    insert(collection)
#####end mongo####

#####begin insert####
def insert(collection):#insert/update into the specified document
    print("in insert")

#get the time in integer format and insert into database
    now_time = int(time.time())
    table = []
    with open ('fake_udp_data.json', 'r') as f:
        for line in f:
            table.append(json.loads(line[18:])) #read in the data after 18 chars

    primary_key = 0
    for row in table:
        print(row)
        print(row['header']['role'])
        curr_time = row['header']['time']
        fromm = row['header']['from']
        role = row['header']['role']
        primary_key = primary_key + 1 #increment the primary key every time a new udp reports comes in
        if role == "resrc":
            print("role == resrc")
            cpus = row['payload']['cpus']
            mems = row['payload']['mems']
            disk = row['payload']['disk']
        #collection.update_one({"udp_report":primary_key}, {"$set": {"payload":
            #{"cpus": cpus, "mems": mems, "disk": disk}}})

            collection.insert({"udp_report":primary_key, "now": now_time, "from": fromm, 
                "role": role, "time": curr_time, "payload": {"cpus": cpus, "mems": mems, "disk": disk}})
        
        elif role == "prepr":
            print("role == prepr")
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
            collection.insert({"udp_report":primary_key,"now": now_time, "from": fromm, 
                "role": role, "time": curr_time, "payload":{"ints": ints, "pcrd": pcrd, "mods": mods, "rcvd": rcdv,
                "prcd": prcd, "invl": invl, "incm": incm, "totl": totl, 
                "genr": genr, "modg": modg, "publ": publ}})

        elif role == "kafka" or role == "druid":
            if role == "kafka":
                print("role == kafka")
            elif role == "druid":
                print("role == druid")
            flow = row['payload']['flow']
            swit = row['payload']['swit']
            anml = row['payload']['anml']
            collection.insert({"udp_report":primary_key,"now": now_time, "from": fromm, 
                "role": role, "time": curr_time, "payload":{"flow": flow, "swit": swit, "anml": anml}})

        elif role == "spark":
            print("role == spark")
            inpt = row['payload']['inpt']
            schd = row['payload']['schd']
            prcs = row['payload']['prcs']
            totl = row['payload']['totl']
            collection.insert({"udp_report":primary_key,"now": now_time, "from": fromm, 
                "role": role, "time": curr_time, "payload":{"inpt": inpt, "schd": schd, "prcs": prcs, "totl": totl}})
    
#####end insert####

#remove a field from a document by unique id
#def delete(collection):

#####begin query()####
def query(collection):
    go = False
    print("Querying...")
    cursor = collection.find({})
    for document in cursor:
        #print("printing...")
        #pprint(document)

        while(go == False):
            time1 = input("Please enter the first time frame(dd/mm/yyyy hh:mm:ss) : ")
            time2 = input("Please enter the second time frame (dd/mm/yyyy hh:mm:ss) ")
            samples = int(input("How many samples to display?: "))

            d1 = d = datetime.strptime(time1, "%d/%m/%Y %H:%M:%S")
            d2 = datetime.strptime(time2, "%d/%m/%Y %H:%M:%S")

            time11 = int(time.mktime(d1.timetuple()))
            time22 = int(time.mktime(d2.timetuple()))

            print(time11)
            print (time22)

            print("What information do you want to get?")
            print("\t resrc")
            print("\t prepr")
            print("\t kafka")
            print("\t druid")
            print("\t spark")

            choice = input("Please enter your choice: ")

            if choice == "resrc":
                print("\t---- resrc ----")
                print("\t     cpus")
                print("\t     mems")
                print("\t     disk")

                payload_info = input("Please enter what payload info you would like to get: ")
                if payload_info == "cpus":
                    L = []
                    print("You entered cpus")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.cpus": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['cpus'])
                    print(L)
                # quit = input("Do you want to exit query? [Y],[N]: ")
                # if quit == "Y" or "y":
                #     go = True
                # elif quit == "N" or "n":
                #     print("no")
                    #exit()
                if payload_info == "mems":
                    L = []
                    print("You entered mems")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.mems": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['mems'])
                    print(L)
                if payload_info == "disk":
                    L = []
                    print("You entered disk")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.disk": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['disk'])
                    print(L)
                    #exit()

                #for data in collection.find({"now": {"$gte": time1, "$lte": time2}}).limit(samples):
    #     pprint(data)

            elif choice == "prepr":
                print("\t---- prepr ----")
                print("\t     ints")
                print("\t     pcrd")
                print("\t     mods")
                print("\t     rcvd")
                print("\t     prcd")
                print("\t     invl")
                print("\t     incm")
                print("\t     totl")
                print("\t     genr")
                print("\t     modg")
                print("\t     publ")

                payload_info = input("Please enter what payload info you would like to get: ")
                if payload_info == "ints":
                    L = []
                    print("You entered ints")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.ints": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['ints'])
                    print(L)
                if payload_info == "pcrd":
                    L = []
                    print("You entered pcrd")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.pcrd": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['pcrd'])
                    print(L)
                if payload_info == "mods":
                    L = []
                    print("You entered mods")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.mods": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['mods'])
                    print(L)
                if payload_info == "rcvd":
                    L = []
                    print("You entered rcvd")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.rcvd": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['rcvd'])
                    print(L)
                if payload_info == "prcd":
                    L = []
                    print("You entered prcd")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.prcd": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['prcd'])
                    print(L)
                if payload_info == "invl":
                    L = []
                    print("You entered invl")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.invl": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['invl'])
                    print(L)
                if payload_info == "incm":
                    L = []
                    print("You entered incm")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.incm": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['incm'])
                    print(L)
                if payload_info == "totl":
                    L = []
                    print("You entered totl")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.totl": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['totl'])
                    print(L)
                if payload_info == "genr":
                    L = []
                    print("You entered genr")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.genr": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['genr'])
                    print(L)
                if payload_info == "modg":
                    L = []
                    print("You entered modg")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.modg": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['modg'])
                    print(L)
                if payload_info == "publ":
                    L = []
                    print("You entered publ")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.publ": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['publ'])
                    print(L)

            elif choice == "kafka" or choice == "druid":
                if choice == "kafka":
                    print("\t---- kafka ----")
                elif choice == "druid":
                    print("\t---- druid ----")
                print("\t     flow")
                print("\t     swit")
                print("\t     anml")
                payload_info = input("Please enter what payload info you would like to get: ")
                if payload_info == "flow":
                    L = []
                    print("You entered flow")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.flow": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['flow'])
                    print(L)
                if payload_info == "swit":
                    L = []
                    print("You entered swit")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.swit": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['swit'])
                    print(L)
                if payload_info == "anml":
                    L = []
                    print("You entered anml")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.anml": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['anml'])
                    print(L)


            elif choice == "sark":
                print("\t---- spark ----")
                print("\t     inpt")
                print("\t     schd")
                print("\t     prcs")
                print("\t     totl")
                payload_info = input("Please enter what payload info you would like to get: ")
                if payload_info == "inpt":
                    L = []
                    print("You entered inpt")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.inpt": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['inpt'])
                    print(L)
                if payload_info == "schd":
                    L = []
                    print("You entered schd")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.schd": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['schd'])
                    print(L)
                if payload_info == "prcs":
                    L = []
                    print("You entered prcs")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.prcs": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['prcs'])
                    print(L)
                if payload_info == "totl":
                    L = []
                    print("You entered totl")
                    for x in collection.find({"time": {"$gte":time11, 
                        "$lte": time22}}, 
                        {"payload.totl": 1, "_id": 0}).limit(samples):
                        #pprint(x['payload']['cpus'])
                        L.append(x['payload']['totl'])
                    print(L)

            else:
                print("Please enter a valid keyword")

####begin menu####
def menu(con, collection):
    go = False
    while (go == False):
        print()
        print("\t (i) insert/update record")
        print("\t (ii) delete/remove record")
        print("\t (iii) query")
        print("\t (iv) display")
        print("\t (v) quit")
        print()

        usr_input = input("Welcome, please enter your choice: ")

        if usr_input == "i":
            print("You chose insert")
            #go = True
            mongo(collection)
        elif usr_input == "ii":
            print("You chose to delete")
            #go = True
            dropDatabase(conn)
        elif usr_input =="iii":
            print("You chose to query")
            #go = True
            query(collection)
        elif usr_input =="iv":
            print("You chose to display")
            #go = True
            displayDatabase(collection)
        elif usr_input == "v":
            print("Quiting...")
            go = True
        else:
            print("Please enter a valid choice from the menu")
            go = False
####end menu####

####begin display()####
def displayDatabase(collection):
    print("Displaying...")
    cursor = collection.find({})
    for document in cursor:
        pprint(document)
####end display()####


####begin dropDatabase()####
def dropDatabase(conn):#drop database for testing purpose
    conn.drop_database("udp_test_db")
    print("Database deleted")
    #exit() #exit script
####end dropDatabase()####

####begin main####
print("begin main")
print("begin mongo connection")
try:
    conn = pymongo.MongoClient()
    print ("Connected successfully!!!")
except pymongo.errors.ConnectionFailure as e:
    print ("Could not connect to MongoDB: %s") % e
conn
#create a database
db = conn.udp_test_db
#create a collection
collection = db.udp_collection
menu(conn, collection)
print("end main")
###end main####

