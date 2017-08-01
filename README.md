•	udpServer.py
•	udp_query.py

ssh lucy server:

-	Start running udpServer.py
-	Start running udp_query.py (flask application)

Query: curl -d '{"from": "20170730110000", "to":"20170731120000", "sample": 121, "role": "kafka"}' -H "Content-Type: application/json" -X POST http://10.201.208.90:5000
