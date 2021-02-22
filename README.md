# MQTT-to-GraphQL-Connector-for-SMP
This is an MQTT client that logs topics to a csv paired with a GraphQL pusher intended for use with CESMII's Smart Manufacturing Platform

All setup is done inside the config.yml file


# Config.yml Description
MQTT_Broker:
IP and Port number of the MQTT broke
#TODO add authentication parameters.....

MQTTSub_Topic:
These are the source topics the MQTT Logger will log. Note The MQTT logger will store all topics detailed here

Topic_toSMP:
These are the topics that will be pushed to the SMP. These topics will be compared against all topics logged and if there is a partial match it will push the topoi

Topic_toSMP_dataType:
These are the DataType the SMP will log the data as

SMP_Identifier:
The unique identifier to determine the source of the data. This is particularly helpful inside the SMP

#TODO allow the addition of custom descriptions per topic

#TODO automated tag mapping to attributes (may be a seperate project)

