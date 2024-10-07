import requests
import json
import pandas as pd
import numpy as np

from datetime import datetime, date

startTime = "timeline"
endTime = "timeline3"
PSED = "Project Start/End Date"
LastMod = "last_updated__1"
BR = "BR#"
PN = "Project # "

datetime_Format = "{:%Y-%m-%d}".format(datetime.now())


api__Key = "MONDAY-API-KEY-HERE"
api__URI = "https://api.monday.com/v2"

monday_headers = {"Authorization" : api__Key,  "API-Version" : "2023-04"}

search__Query_board_items = 'query {boards (ids: 6739613863) {groups {id title}}}' #Queries all groups in a board. Includes groupID which you can use to query next variable.
data_board_item = {'query': search__Query_board_items}
Query_boardItems = requests.post(url=api__URI, json=data_board_item, headers=monday_headers)
json_data_boarditems = json.loads(Query_boardItems.text)
parsed_JSON__boardItem = pd.json_normalize(json_data_boarditems['data']['boards'][0]['groups'])
parsed_groupings = parsed_JSON__boardItem[parsed_JSON__boardItem['title'].str.contains(BR,PN)]

parsed_groupings['Project Number'] = (parsed_groupings['title'].str.split('Project # ').str[1])


for proj_number_iterator, project_variable in zip(parsed_groupings['Project Number'], parsed_groupings['id']):
    search__UpdateLastMod = 'query {boards (ids: 6739613863) {groups (ids:' + '"' + project_variable +'"' + '){ id items_page(limit: 100) {cursor items {id name column_values {id  value}}}}}}'
    data_UpdateLastMod = {'query': search__UpdateLastMod}
    Query_UpdateLastMod = requests.post(url=api__URI, json=data_UpdateLastMod, headers=monday_headers)
    json_data_UpdateLastMod = json.loads(Query_UpdateLastMod.text)
    parsed_json_UpdateLastMod = pd.json_normalize(json_data_UpdateLastMod['data']['boards'][0]['groups'][0]['items_page']['items'],record_path='column_values', meta=['name']).dropna()
    parsed_project_endDate = parsed_json_UpdateLastMod.loc[parsed_json_UpdateLastMod['name'].isin([PSED])]
    JSON_board_Value_PSED = (parsed_project_endDate.loc[parsed_project_endDate['id'].isin([startTime,endTime])])

    valueJSON = JSON_board_Value_PSED['value']

    def parse_value(value):
        return json.loads(value)

    parsed_JSON_object = valueJSON.apply(parse_value)
    JSON_Value_print = pd.json_normalize(parsed_JSON_object)
    concat_ID_JSON = pd.concat([JSON_board_Value_PSED.reset_index(drop=True), JSON_Value_print.reset_index(drop=True)], axis=1)
    concat_ID_JSON.drop(columns='value', inplace=True)


    if concat_ID_JSON.empty:
        print('df is empty -' + ' ' + str(proj_number_iterator))
        pass
    else:
        changed_today_columns = (concat_ID_JSON.loc[concat_ID_JSON['changed_at'].str.contains(datetime_Format)])
        changed_today_columns = changed_today_columns.drop(['name','to'], axis=1)
        changed_today_columns.to_csv('result'+'_' + datetime_Format + " " + proj_number_iterator +  '.csv', index=False)
        if changed_today_columns.empty:
            pass
        else:
            ivanti_url = "https://demo.saasit.com/api/odata/businessobject/Frs_Projects?$filter=ProjectNumber eq" + " " + proj_number_iterator
            print(ivanti_url)
            iv_auth_header = {'Authorization': 'rest_api_key=IVNT-API-KEY-HERE'}

            request_iv_get_recID = requests.get(url=ivanti_url, headers=iv_auth_header)
            request_iv_get_recID_text = request_iv_get_recID.text
            json_RecID = json.loads(request_iv_get_recID_text)

            for test in json_RecID['value']:
                RecId = (test['RecId'])

            for startEndDateVariable, actualDate in zip(changed_today_columns['id'], changed_today_columns['from']):
                if startEndDateVariable == 'timeline3':
                    ivanti_mod_url = "https://demo.saasit.com/api/odata/businessobject/Frs_Projects('" + RecId + "')"
                    datetime_Format_ivntformatted = actualDate + "T00:00:00Z"
                    body_end_Date = "{'ProjectEndDate': " + "'" + datetime_Format_ivntformatted+ "'}"
                    print(body_end_Date)
                    response = requests.put(url=ivanti_mod_url, data=body_end_Date, headers=iv_auth_header)
                else:
                    if startEndDateVariable == 'timeline':
                        ivanti_mod_url = "https://demo.saasit.com/api/odata/businessobject/Frs_Projects('" + RecId + "')"
                        print(ivanti_mod_url)
                        datetime_Format_ivntformatted = actualDate + "T00:00:00Z"
                        body_start_Date = "{'ProjectStartDate': " + "'" + datetime_Format_ivntformatted+ "'}"
                        print(body_start_Date)
                        response = requests.put(url=ivanti_mod_url, data=body_start_Date, headers=iv_auth_header)
                    else:
                        print("idk..")
                        changed_today_columns.to_csv('result_ERROR'+'_' + datetime_Format + '.csv', index=False)

        





