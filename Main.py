import MQTT_logger
import SMplatform as smp
import yaml, glob, os, time, sys, calendar, dotenv
import msvcrt
import threading

dotenv.load_dotenv()

#get authentication from SMP
def SMP_auth():
    authenticator = os.environ.get("authenticator")
    pw = os.environ.get("pw")
    user = os.environ.get("user")
    role = os.environ.get("role")
    token = smp.get_token(authenticator, pw, user, endpoint_url, role)
    header = {"Authorization": token}

    return header

def getOldest(Directory):
    for file in glob.glob(Directory + "\\*.csv"):
        file_list.append(file)

    if(file_list != []):  
        oldest_file = min(file_list, key=os.path.getctime)
        print(oldest_file)
        return os.path.splitext(os.path.basename(oldest_file))[0] + '.csv'
    else:
        print("No Files Present... Sleeping for 1 second...")
        time.sleep(1)
        return None

def background_MQTT_Logger_Start():
    MQTT_logger.Start()

def ConnectionStatus_SMP(ConnectionStatus, config, Connector_Identifier, endpoint_url, header, verbose=False):
    #QoD messaging?
    print(f"executeing connection status ({ConnectionStatus}) mutation...")

    tag_info = []
    numberOfTopics = len(config['Topic_toSMP'])

    for x in range(0, numberOfTopics):
        FQ_Tag = f'''{Connector_Identifier}.{''.join(config['Topic_toSMP'][x])}'''
        tag_info += [smp.findTagID_Create(Connector_Identifier, FQ_Tag, config['Topic_toSMP_dataType'][x] , endpoint_url, header, create = True, verbose = False)]

    #TODO Need to check if this is proper ~~~~~~~~~~   
    for i in tag_info:
        mutation = smp.build_UpdateTagTS_Mutation(i[1], "null", time="now", status=ConnectionStatus) 
        if(verbose==True):print(mutation)
        result = smp.request(mutation, endpoint_url, header)

        if 'errors' in result.keys():
                raise Exception(result)
                sys.exit()
                #TODO error handling
    #TODO~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    return tag_info

#read config from config.yml
try:
    with open("config.yml", 'r') as data:
        config = yaml.safe_load(data)
except Exception:
    print("error importing config.yml")
    exit()

#SMP header setup
endpoint_url = "https://rtccam.cesmii.net/graphql"
header = SMP_auth()
Connector_Identifier = "MQTT_Connector"

#File Organization
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
Source_DIR = f'{ROOT_DIR}\\MQTT_Logged\\'
Destination_DIR = f'{ROOT_DIR}\\uploaded\\'

#Initialize Variables
file_list=[]
Last_Run = False

#Kick off the MQTT Logger
Enable_MQTT_Logger = input('Start MQTT Logger? (y/n)')
if(Enable_MQTT_Logger == 'y'):
    mqttclientThread = threading.Thread(target=background_MQTT_Logger_Start) 
    mqttclientThread.daemon = True
    mqttclientThread.start()

#UncertainInitialValue HEX 40920000 -> 1083310080
#tag_info = ConnectionStatus_SMP('1083310080', config, Connector_Identifier, endpoint_url, header)
#SMP doesnt deal with reconnection status
tag_info = []

while True:    
    #Check if this is the last run and Source Directory has any CSV files
    if (Last_Run==True and any(".csv" in s for s in os.listdir(Source_DIR)) == False):
        #grab last logged data
        #TODO Search for any csv file instead of 'backlog.csv'???
        file = 'backlog.csv'
        
        if(os.path.exists(f'{ROOT_DIR}\\MQTT_Logging\\{file}')): 
            print("then this happens.....")
            os.replace(f'{ROOT_DIR}\\MQTT_Logging\\{file}', f'{ROOT_DIR}\\pushing\\{file}')
    else:
        #Select Oldest File 
        file = getOldest(Source_DIR)
        if(file != None): os.replace(f'{Source_DIR}{file}', f'{ROOT_DIR}\\pushing\\{file}')

    current_file = f'{ROOT_DIR}\\pushing\\{file}'

    if(os.path.exists(current_file) and file != None):

        print("Pushing: " + str(file))
        entries = smp.build_entries(config, current_file)
        numberOfTopics = len(config['Topic_toSMP'])

        for x in range(0, numberOfTopics):
            FQ_Tag = f'''{Connector_Identifier}.{''.join(config['Topic_toSMP'][x])}'''
            tag_info += [smp.findTagID_Create(Connector_Identifier, FQ_Tag, config['Topic_toSMP_dataType'][x] , endpoint_url, header, create = True, verbose = False)]

            if(entries[x] != ''):
                print("these are the entries" + entries[x])
                mutation = smp.build_UpdateMultipleTagTS_Mutation(tag_info[x][1], entries[x])
                print(mutation)
                result = smp.request(mutation, endpoint_url, header) # Execute the mutation

                #check for errors
                if 'errors' in result.keys():
                    raise Exception(result)
                    sys.exit()
            else:
                print(f"nothing to push for: {FQ_Tag}")

        if file == 'backlog.csv':
            timestamp_epoch = calendar.timegm(time.localtime())
            file = f"backlog{timestamp_epoch}.csv"
            os.rename(current_file,rf"{ROOT_DIR}\\pushing\\{file}")


        os.rename(f'{ROOT_DIR}\\pushing\\{file}', f'{Destination_DIR}{file}')
    
    file_list=[]

    if Last_Run==True:
        break

    #if enter hit exit code
    #note this only works on microsoft.... may need to rethink
    if msvcrt.kbhit(): 
        if msvcrt.getwche() == '\r': 
            print("Exiting Connecter")
            Last_Run = True

            if(Enable_MQTT_Logger == 'y'):
                print('Stopping MQTT Logger')
                MQTT_logger.Stop()
                #TODO confirm MQTT_Logger is off? Sleep 0.5 as bandaid for now
                time.sleep(0.5)
            


# I think this would be better: BadConnectionClosed HEX 80AE0000 -> 2158886912
# CESMII uses: BAD HEX 80000000 -> 2147483648
tag_info = ConnectionStatus_SMP('2147483648', config, Connector_Identifier, endpoint_url, header)

print('Exiting Program...')
sys.exit()