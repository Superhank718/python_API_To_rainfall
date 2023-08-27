# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


import requests
import pymongo as pmg 
#載入pymongo模組

client = pmg.MongoClient()
db = client.TestDB_1

Weather_Data = db.py_collection_2

# 定義API網址
api_url = "https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0002-001"

# 設定授權碼
authorization = "rdec-key-123-45678-011121314"

# 設定查詢參數
query_params = {
    "Authorization": authorization
}

# 發送API請求
response = requests.get(api_url, params=query_params)




# 檢查API回應狀態碼
if response.status_code == 200:
    # 解析JSON回應
    data = response.json()

    
    locations = data["records"]["location"]
    
    for location in locations:
        lat = location["lat"]
        lon = location["lon"]
        location_name = location["locationName"]
        station_id = location["stationId"]
        obs_time = location["time"]["obsTime"]

        weather_elements = {}
        for element in location["weatherElement"]:
            element_name = element["elementName"]
            element_value = float(element["elementValue"]["value"]) if isinstance(element["elementValue"], dict) else float(element["elementValue"])
            weather_elements[element_name] = element_value

        parameters = {}
        for parameter in location["parameter"]:
            parameter_name = parameter["parameterName"]
            parameter_value = parameter["parameterValue"]
            parameters[parameter_name] = parameter_value

        document = {
            "lat": lat,
            "lon": lon,
            "locationName": location_name,
            "stationId": station_id,
            "obsTime": obs_time,
            "weatherElements": weather_elements,
            "parameters": parameters
        }
        Weather_Data.insert_one(document)

    # 搜尋條件
    query = {
        "weatherElements.latest_3days": {"$gt": 100}
    }
    # 投影，只顯示需要的欄位
    projection = {
        "_id": 0,
        "lat": 1,
        "lon": 1,
        "parameters.CITY": 1,
        "parameters.TOWN": 1,
        "weatherElements.latest_3days": 1
    }
    # 執行查詢
    results = Weather_Data.find(query, projection)

    # 顯示結果
    for result in results:
        lat = result["lat"]
        lon = result["lon"]
        city = result["parameters"]["CITY"]
        town = result["parameters"]["TOWN"]
        rainfall_3days = result["weatherElements"]["latest_3days"]

        print(f"經度: {lon}, 緯度: {lat}")
        print(f"縣市: {city}, 鄉鎮: {town}")
        print(f"過去三天累積雨量: {rainfall_3days} 毫米")
        print("--------------")
else:
    print("API請求失敗")


#mongo_data = Weather_Data.delete_many({})
#清除Weather_Data的資料

#for i in Weather_Data.find({}):
#    print(i)
#顯示Weather_Data內的資料
##################################