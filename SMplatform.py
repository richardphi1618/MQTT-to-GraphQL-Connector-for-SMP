import requests
import json, csv, os

def request(content, url, headers=None, verbose = False):
    if(verbose == True): print(content)
    r = requests.post(url, headers=headers, data={"query": content})
    r.raise_for_status()
    if(verbose):print(r.json())
    return r.json()

def get_token (auth, password, name, url, role):
    
    response = request(f'''
    mutation authRequest {{
        authenticationRequest(input: {{authenticator: "{auth}", role: "{role}", userName: "{name}"}}) {{
            jwtRequest {{
            challenge, message
            }}
        }}
    }}
    ''', url) 

    jwt_request = response['data']['authenticationRequest']['jwtRequest']

    if jwt_request['challenge'] is None:
        raise Exception(jwt_request['message'])
    else:
        response=request(f'''
        mutation authValidation {{
            authenticationValidation( input: {{authenticator: "{auth}", signedChallenge: "{jwt_request["challenge"]}|{password}"}} ){{
                jwtClaim
            }}
        }}
        ''', url)

    jwt_claim = response['data']['authenticationValidation']['jwtClaim']
    return f"Bearer {jwt_claim}"

def build_tsData_Query(tagID, start = "2020-12-1T00:00:00", end = "now", maxSamples=0):

    query_string = f'''
    query tsData_Query {{
        getRawHistoryDataWithSampling(
            startTime: "{start}"
            endTime: "{end}"
            ids: {tagID}
            maxSamples: {maxSamples}
        ) {{
            id
            ts
            boolvalue
            floatvalue
            intvalue
            stringvalue
            dataType
        }}
    }}
    '''
    return query_string

def build_RunIDts_Query(runID , startTime = "2020-12-1T00:00:00", endTime = "now"):
    query_string = f'''
    query RunIDts_Query {{
        getRawHistoryDataWithSampling(
            ids: "6530"
            endTime: "{endTime}"
            maxSamples: 0
            filter: {{ stringvalue: {{ {runID} }} }}
            startTime: "{startTime}"
        ) {{
        ts
        stringvalue
        }}
    }}
    '''
    return query_string

def getStartandEndTime(runID, endpoint_url, header, verbose=False):
    StartandEnd=['','']

    my_query = build_RunIDts_Query(f"equalTo: \"{runID}\" ")
    if(verbose):print(my_query)
    result = request(my_query, endpoint_url, header) # Execute the query
    StartandEnd[0]= result['data']['getRawHistoryDataWithSampling'][0]['ts'] #save the start time
    if(verbose):print(result)

    my_query = build_RunIDts_Query("equalTo: \"0\"",StartandEnd[0]) #search for the finish
    if(verbose):print(my_query)
    result = request(my_query, endpoint_url, header) # Execute the query
    StartandEnd[1]= result['data']['getRawHistoryDataWithSampling'][0]['ts']
    if(verbose):print(result)

    return StartandEnd

def build_TagList_Query(name):
    query_string = f'''
    query TagList_Query {{
        tags(filter: {{ displayName: {{ startsWith: "{name}" }} }}) {{
            id
            displayName
            description
            dataType
        }}
    }}
    '''
    return query_string

def build_CreateTag_Mutation(name, dataType="STRING", desc = "", partOfId = "214"):
    query_string = f'''
    mutation CreateTag_Mutation {{
        createTag(
            input: {{
                tag: {{
                dataType: {dataType}
                description: "{desc}"
                displayName: "{name}"
                relativeName: "{name}"
                partOfId:"{partOfId}"
            }}
        }}
    )   {{
        clientMutationId
        }}
    }}
    '''
    return query_string

def build_UpdateTagTS_Mutation(tagID, value, time="now", status="0"):
    query_string = f'''
    mutation UpdateTagTS_Mutation {{
        replaceTimeSeriesRange(
            input: {{
            attributeOrTagId: "{tagID}"
            entries: {{ value: {value}, timestamp: "{time}", status: "{status}" }}
            }}
        ) {{
          clientMutationId
          json
        }}
    }}
    '''
    return query_string

def build_UpdateMultipleTagTS_Mutation(tagID, entries):
    query_string = f'''
    mutation UpdateTagTS_Mutation {{
        replaceTimeSeriesRange(
            input: {{
            attributeOrTagId: "{tagID}"
            entries: [{entries}]
            }}
        ) {{
          clientMutationId
          json
        }}
    }}
    '''
    return query_string

def findTagID_Create (Tag_Identifier, tagName, dataType, endpoint_url, header, desc= "This was made using Python", create= True, verbose = False):
    # Tag Identifier should proceed the tag
    # This is usually in reference to the connector being used
    # example: tagIdentifier.tagName, MQTT_Connector.ccam_sim_runID

    if(verbose==True):print("find/create Tag ID start ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    my_query = build_TagList_Query(Tag_Identifier)
    if(verbose==True):print(my_query)
    result = request(my_query, endpoint_url, header)
    tagList = result.get('data').get('tags')

    tagID = None

    #search for tag in list
    for i in tagList:
        if i['displayName'] == tagName:
            tagID= i['id']

    
    #if tag not present create tag
    if(create == True):
        if (tagID == None):
            if(verbose==True):print("\ntag not in list and will be added\n")
            my_Mutation = build_CreateTag_Mutation(tagName, dataType, desc)
            if(verbose==True):print(my_Mutation)
            result = request(my_Mutation, endpoint_url, header)
            if(verbose==True):print(result)
            result = request(my_query, endpoint_url, header)
            tagList = result.get('data').get('tags')

            for i in tagList:
                if i['displayName'] == tagName:
                    tagID= i['id']
    else:
        if(tagID == None):
            if(verbose==True):print("No Tag found and will not create")
            return


    if(verbose==True):print(f"\n{tagName} tagID: {tagID}\n")

    if(verbose==True):print("find/create Tag ID end ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    return [tagName, tagID]

def contains(small, big):
    for i in range(len(big)-len(small)+1):
        for j in range(len(small)):
            if big[i+j] != small[j]:
                break
        else:
            return True
    return False

def remove_dup(a):
   i = 0
   while i < len(a):
      j = i + 1
      while j < len(a):
         if a[i] == a[j]:
            del a[j]
         else:
            j += 1
      i += 1

def build_entries(config, file, verbose=False):

    final_entries = [None] * len(config['Topic_toSMP'])
    topic_count = 0


    for t in config['Topic_toSMP']:
        if(file != None):
            entries = []

            if(verbose==True):
                print('\n************************************************************************')
                print(f'''Uploading topic: {t} \nFrom: {file}''') 
                print('\n************************************************************************')

            with open(file, newline='') as f:
                reader = csv.reader(f)
                data = list(reader)

                #TODO confirm there isnt a bug in the MQTT Logger
                #scan list for duplicates
                remove_dup(data)

            for x in data:
                if (x != []):
                    y = x[0].split('_')
                    if (contains(t,y)):
                        dp = f'''{{value: "{x[1]}", timestamp: "{x[2]}", status: "0"}}'''
                        entries += [dp]
                        if(verbose==True): print(dp)
                        if(verbose==True): print(x)


        entries = ','.join(entries)
        final_entries[topic_count] = entries
        topic_count += 1


    if(verbose==True):print(final_entries)

    return final_entries

if __name__ == '__main__':
    endpoint_url = os.environ.get("endpoint_url")
    authenticator = os.environ.get("authenticator")
    pw = os.environ.get("pw")
    user = os.environ.get("user")
    role = os.environ.get("role")
    header = {"Authorization":f"{get_token(authenticator, pw, user, endpoint_url, role)}"}
    print(header)