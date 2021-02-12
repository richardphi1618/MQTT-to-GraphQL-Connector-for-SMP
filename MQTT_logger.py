import os, time, yaml, calendar, dotenv
import datetime as DT
import paho.mqtt.client as mqtt
import csv
from pathlib import Path

def current_milli_time():
    return round(time.time() * 1000)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.

    client.subscribe("$SYS/#")

    print(config['MQTTSub_Topic'])
    for i in config['MQTTSub_Topic']:
        print(i)
        client.subscribe(i)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    timestamp_epoch = current_milli_time()
    timestamp_iso = DT.datetime.fromtimestamp(timestamp_epoch/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
    #print(f"{msg.topic} {str(msg.payload)} @time of arrival:{timestamp_iso}")
    payload = str(msg.payload)
    topic = str(msg.topic)
    fields=[topic.replace("/", "_"), payload.strip('b\'').rstrip('\''),timestamp_iso +"-05:00"]

    with open(working_file,'a') as f:
        writer = csv.writer(f)
        writer.writerow(fields)

def Start (verbose = False):

    #read config from config.yml
    try:
        with open("config.yml", 'r') as data:
            global config 
            config = yaml.safe_load(data)
    except Exception:
        print("error importing config.yml")
        exit()

    #setting up MQTT communication
    Broker_URL = "mqtt://" + config['MQTT_Broker'][0] + ":" + config['MQTT_Broker'][1]
    if(verbose==True):print ("Broker URL -> " + Broker_URL)
    
    #Load MQTT topics to monitor
    for i in config['MQTTSub_Topic']:
        if(verbose==True):print ("Topic -> " + i)

    #Init MQTT Client
    global client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(config['MQTT_Broker'][0], int(config['MQTT_Broker'][1]), 60)
    client.loop_start()

    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    global working_file
    working_file = f'{ROOT_DIR}/MQTT_logging/backlog.csv'
    
    timestamp_epoch_logStart = calendar.timegm(time.localtime())
    LoggingTimerLength = 10 #10 seconds
    MaxFileSize = 50000 # 50 KB
    
    while True:
        try:
            Path(working_file).stat()
        except (FileNotFoundError, IOError):
            if(verbose==True):print("Creating Log")
            with open(working_file, 'w') as fp: 
                pass
        
        file_size=Path(working_file).stat().st_size
        
        if((timestamp_epoch_logStart+LoggingTimerLength) <= calendar.timegm(time.localtime()) or file_size >= MaxFileSize):
            print(timestamp_epoch_logStart+LoggingTimerLength)
            print(calendar.timegm(time.localtime()))
            if(verbose==True):print("\nfile needs to be pushed.... stopping client and renaming backlog")
            timestamp_epoch_logStop = calendar.timegm(time.localtime())
            client.loop_stop()
            os.rename(working_file,rf"{ROOT_DIR}/MQTT_logged/backlog{timestamp_epoch_logStop}.csv")
            timestamp_epoch_logStart = timestamp_epoch_logStop
            client.loop_start()


def Stop():
    client.loop_stop()
    print("MQTT Logger Stopped")
    return None

if __name__ == '__main__':
    Start(verbose = True)

        

    



