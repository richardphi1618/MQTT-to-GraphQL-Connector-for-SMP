# MQTT-to-GraphQL-Connector-for-SMP
This is an MQTT client that logs topics to a csv paired with a GraphQL pusher intended for use with CESMII's Smart Manufacturing Platform

All setup is done inside the config.yml file


## Config.yml Description

Line in _config.yml_  | Description
------------- | -------------
MQTT_Broker: | IP and Port number of the MQTT broke
MQTTSub_Topic: | These are the source topics the MQTT Logger will log. Note The MQTT logger will store all topics detailed here
Topic_toSMP: | These are the topics that will be pushed to the SMP. These topics will be compared against all topics logged and if there is a partial match it will push the topic
Topic_toSMP_dataType: | These are the DataType the SMP will log the data as
SMP_Identifier: | The unique identifier to determine the source of the data. This is particularly helpful inside the SMP

## .env Description (authentication)
You will need to build your own .env file and fill it with your own authentication settings

Line in _.env_  | Description
------------- | -------------
endpoint_url = "https://rtccam.cesmii.net/graphql" | _this is the endpoint for the CESMII platform (example shown)_
authenticator = "************" | _this the authenticator the site admin sets up_
pw = "************" | _password used_
user = "Richard.Blanchette" | _this is for logging purposes only_
role = "rtccm_graphql" | _the roll you wish to use with the given authenticator_

## _#TODO_ 
 - [ ] add authentication parameters for secure MQTT broker access
 - [ ] allow the addition of custom descriptions per topic
 - [ ] automated tag mapping to attributes (may be a seperate project)

