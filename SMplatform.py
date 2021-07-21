import csv
import os
from typing import Optional

import dotenv
import requests


def request(
    content: str, url: str, headers: dict = None, verbose: bool = False
) -> dict:
    """Execute request with verbose wrapper"""
    if verbose:
        print(content)
    r = requests.post(url, headers=headers, data={"query": content})
    r.raise_for_status()
    if verbose:
        print(r.json())
    return r.json()


def get_token(auth: str, password: str, name: str, url: str, role: str) -> str:
    """Get JWT"""
    response = request(
        f"""
    mutation authRequest {{
        authenticationRequest(input: {{authenticator: "{auth}", role: "{role}", userName: "{name}"}}) {{
            jwtRequest {{
            challenge, message
            }}
        }}
    }}
    """,
        url,
    )

    jwt_request = response["data"]["authenticationRequest"]["jwtRequest"]

    if jwt_request["challenge"] is None:
        raise Exception(jwt_request["message"])
    else:
        response = request(
            f"""
        mutation authValidation {{
            authenticationValidation( input: {{
                authenticator: "{auth}", signedChallenge: "{jwt_request["challenge"]}|{password}"
                }} )
                {{jwtClaim}}
        }}
        """,
            url,
        )

    jwt_claim = response["data"]["authenticationValidation"]["jwtClaim"]
    return f"Bearer {jwt_claim}"


def SMP_auth() -> dict:
    """SMIP Authentication"""
    dotenv.load_dotenv()
    endpoint_url = str(os.environ.get("endpoint_url"))
    authenticator = str(os.environ.get("authenticator"))
    pw = str(os.environ.get("pw"))
    user = str(os.environ.get("user"))
    role = str(os.environ.get("role"))
    token = str(get_token(authenticator, pw, user, endpoint_url, role))
    header = {"Authorization": token}

    return header


def build_tsData_Query(
    tagID: str, start: str = "2020-12-1T00:00:00", end: str = "now", maxSamples: int = 0
) -> str:
    """Build query for time series data"""
    query_string = f"""
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
    """
    return query_string


def build_RunIDts_Query(
    runID: str, startTime: str = "2020-12-1T00:00:00", endTime: str = "now"
) -> str:
    """Build query for Run ID data"""
    query_string = f"""
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
    """
    return query_string


def getStartandEndTime(
    runID: str, endpoint_url: str, header: dict, verbose: bool = False
) -> list[str]:
    """Get start and end times for specific runID"""
    StartandEnd = ["", ""]

    my_query = build_RunIDts_Query(f'equalTo: "{runID}" ')
    if verbose:
        print(my_query)

    # Execute the query
    result = request(my_query, endpoint_url, header)

    # save the start time
    StartandEnd[0] = result["data"]["getRawHistoryDataWithSampling"][0]["ts"]
    if verbose:
        print(result)

    # search for the finish
    my_query = build_RunIDts_Query('equalTo: "0"', StartandEnd[0])
    if verbose:
        print(my_query)

    # Execute the query
    result = request(my_query, endpoint_url, header)
    StartandEnd[1] = result["data"]["getRawHistoryDataWithSampling"][0]["ts"]
    if verbose:
        print(result)

    return StartandEnd


def build_TagList_Query(name: str) -> str:
    """Build query for returning tag list"""
    query_string = f"""
    query TagList_Query {{
        tags(filter: {{ displayName: {{ startsWith: "{name}" }} }}) {{
            id
            displayName
            description
            dataType
        }}
    }}
    """
    return query_string


def build_CreateTag_Mutation(
    name: str, dataType: str = "STRING", desc: str = "", partOfId: str = "214"
) -> str:
    """Build mutation for creating a new tag"""
    query_string = f"""
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
    """
    return query_string


def build_UpdateTagTS_Mutation(
    tagID: str, value: str, time: str = "now", status: str = "0"
) -> str:
    """Build mutation for updating time series for a specific tag ID"""
    query_string = f"""
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
    """
    return query_string


def build_UpdateMultipleTagTS_Mutation(tagID: str, entries: str) -> str:
    """Build mutation for updating multiple tags with multiple timeseries data"""
    # this came about through experimentation. Instead of deleting build_UpdateTagTS_Mutation I kept it
    query_string = f"""
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
    """
    return query_string


def findTagID_Create(
    Tag_Identifier: str,
    tagName: str,
    dataType: str,
    endpoint_url: str,
    header: dict,
    desc: str = "This was made using Python",
    create: bool = True,
    verbose: bool = False,
) -> list[Optional[str]]:
    """Find a tagID and create it if it does not already exist"""
    # Tag Identifier should proceed the tag
    # This is usually in reference to the connector being used
    # example: tagIdentifier.tagName, MQTT_Connector.ccam_sim_runID

    if verbose:
        print("find/create Tag ID start ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    my_query = build_TagList_Query(Tag_Identifier)
    if verbose:
        print(my_query)
    result = request(my_query, endpoint_url, header)
    tagList = result["data"].get("tags")

    tagID = None

    # search for tag in list
    for i in tagList:
        if i["displayName"] == tagName:
            tagID = i["id"]

    # if tag not present create tag
    if tagID is None:
        if create:
            my_Mutation = build_CreateTag_Mutation(tagName, dataType, desc)
            result = request(my_Mutation, endpoint_url, header)

            if verbose:
                print("\ntag not in list and will be added\n")
                print(my_Mutation)
                print(result)

            result = request(my_query, endpoint_url, header)
            tagList = result["data"].get("tags")

            for i in tagList:
                if i["displayName"] == tagName:
                    tagID = i["id"]
        else:
            if verbose:
                print("No Tag found and will not create")
            return [tagName, tagID]

    if verbose:
        print(f"\n{tagName} tagID: {tagID}\n")

    if verbose:
        print("find/create Tag ID end ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    return [tagName, tagID]


def contains(small: str, big: list) -> bool:
    """Return bool if small is in big"""
    for i in range(len(big) - len(small) + 1):
        for j in range(len(small)):
            if big[i + j] != small[j]:
                break
        else:
            return True
    return False


def remove_dup(a: list) -> None:
    """Removes duplicates from list"""
    i = 0
    while i < len(a):
        j = i + 1
        while j < len(a):
            if a[i] == a[j]:
                del a[j]
            else:
                j += 1
        i += 1


def build_entries(config: dict, file: str, verbose: bool = False) -> list[str]:
    """Build entries for batch upload to SMIP"""
    final_entries = [""] * len(config["Topic_toSMP"])

    for idx, t in enumerate(config["Topic_toSMP"]):
        if file is not None:
            entries = []

            if verbose:
                print(
                    "\n************************************************************************"
                )
                print(f"""Uploading topic: {t} \nFrom: {file}""")
                print(
                    "\n************************************************************************"
                )

            with open(file, newline="") as f:
                reader = csv.reader(f)
                data = list(reader)
                remove_dup(data)

            for x in data:
                if x != []:
                    y = x[0].split("_")
                    if contains(t, y):
                        dp = (
                            f"""{{value: "{x[1]}", timestamp: "{x[2]}", status: "0"}}"""
                        )
                        entries += [dp]
                        if verbose:
                            print(dp)
                        if verbose:
                            print(x)

        entries = ",".join(entries)
        final_entries[idx] = str(entries)

    if verbose:
        print(final_entries)

    return final_entries


if __name__ == "__main__":
    """Request JWT"""
    # SMP header setup
    header = SMP_auth()
    print(header)
