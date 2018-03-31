import socket
import sys
import requests
import json
import csv

def send_data_to_spark(tcp_connection, callsign_dict):
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket.connect(('pub-vrs.adsbexchange.com', 32010))

    airline_dict = {}

    depth = 0
    buffer = ''
    struct = True
    while True:
        byte = clientsocket.recv(1)
        buffer += byte
        
        if byte == '{' and struct:
            depth += 1

            # Handling JSON format transmission issue
            if depth > 2:
                print "Broken JSON, reseting stream"
                clientsocket.close()
                clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                clientsocket.connect(('pub-vrs.adsbexchange.com', 32010))
                depth = 0
                buffer = ''
                struct = True
                
        elif byte == '}' and struct:
            depth -=1
            if depth == 0:
                parse_individual(buffer, tcp_connection, airline_dict, callsign_dict)
                buffer = ''
        elif byte == '"' and buffer[-2] != '\\':
            struct = not struct
    

def parse_individual(data, tcp_connection, airline_dict, callsign_dict):
    try:
        full_struct = json.loads(data)
        acList = full_struct['acList']
        for ac in acList:
            output = ''
            icao = ''
            lat = ''
            lon = ''
            spd = ''
            alt = ''
            call = ''
            iata_call = ''
            if 'Icao' in ac:
                icao = ac['Icao']
            if 'Lat' in ac:
                lat = str(ac['Lat'])
            if 'Long' in ac:
                lon = str(ac['Long'])
            if 'Spd' in ac:
                spd = str(ac['Spd'])
            if 'Alt' in ac:
                alt = str(ac['Alt'])
            if 'Call' in ac:
                call = ac['Call']
                airline_dict[icao] = call
            else:
                if icao in airline_dict:
                    call = airline_dict[icao]

            if call != '' and len(call) > 3:
                if call[:3] in callsign_dict:
                    iata_call = callsign_dict[call[:3]] + call[3:]
                    
            output = icao + ',' + lat + ',' + lon + ',' + spd + ',' + alt + ',' + call + ',' + iata_call
            tcp_connection.send(output + '\n')
    except Exception, e:
        print "An error occured " + str(e)


#Load airline callsign mapping
callsign_dict = {}

with open('airlines.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        callsign_dict[row['icao']] = row['iata']

#Open TCP connection
TCP_IP = "localhost"
TCP_PORT = 9008
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting ADS-B Data.")
send_data_to_spark(conn, callsign_dict)
