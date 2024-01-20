import os
import json
import pandas as pd
import psycopg2

#Aggregated Transaction extraction

path1 = "C:/Users/Admin/Documents/GUVI/phonepe/pulse/data/aggregated/transaction/country/india/state/"
aggregatedTransactionList = os.listdir(path1)

columns1 = {"States" : [],
           "Year" :[],
           "Quater" :[],
           "TransactionType" :[],
           "TransactionCount" : [],
           "TransactionAmount" : []}

for state in aggregatedTransactionList:
    currentState = path1+state+"/"
    aggregatedYearList = os.listdir(currentState)

    for year in aggregatedYearList:
        currentYear = currentState+year+"/"
        aggregatedFileList = os.listdir(currentYear)

        for file in aggregatedFileList:
            currentFile = currentYear+file
            data = open(currentFile,"r")

            loadJson = json.load(data)

            for i in loadJson['data']['transactionData']:
                name = i["name"]
                count = i['paymentInstruments'][0]["count"]
                amount = i['paymentInstruments'][0]["amount"]
                columns1["TransactionType"].append(name)
                columns1["TransactionCount"].append(count)
                columns1["TransactionAmount"].append(amount)
                columns1["States"].append(state)
                columns1["Year"].append(year)
                columns1["Quater"].append(int(file.strip(".json")))

aggregatedTransactionDF = pd.DataFrame(columns1)

aggregatedTransactionDF["States"] = aggregatedTransactionDF["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggregatedTransactionDF["States"] = aggregatedTransactionDF["States"].str.replace("-"," ")
aggregatedTransactionDF["States"] = aggregatedTransactionDF["States"].str.title()
aggregatedTransactionDF['States'] = aggregatedTransactionDF['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#Aggregated User extraction

path2 = "C:/Users/Admin/Documents/GUVI/phonepe/pulse/data/aggregated/user/country/india/state/"
aggregatedUserList = os.listdir(path2)

columns2 = {"States" : [],
           "Year" :[],
           "Quater" :[],
           "UserBrand" :[],
           "UserCount" : [],
           "UserPercentage" : []}

for state in aggregatedUserList:
    currentState = path2+state+"/"
    aggregatedYearList = os.listdir(currentState)

    for year in aggregatedYearList:
        currentYear = currentState+year+"/"
        aggregatedFileList = os.listdir(currentYear)

        for file in aggregatedFileList:
            currentFile = currentYear+file
            data = open(currentFile,"r")

            loadJson = json.load(data)

            try:
                for i in loadJson["data"]["usersByDevice"]:
                    brand = i["brand"] 
                    count = i["count"]
                    percentage = i["percentage"]
                    columns2["UserBrand"].append(brand)
                    columns2["UserCount"].append(count)
                    columns2["UserPercentage"].append(percentage)
                    columns2["States"].append(state)
                    columns2["Year"].append(year)
                    columns2["Quater"].append(int(file.strip(".json")))
            except:
                pass

aggregatedUserDF = pd.DataFrame(columns2)
print(aggregatedUserDF)

aggregatedUserDF["States"] = aggregatedUserDF["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggregatedUserDF["States"] = aggregatedUserDF["States"].str.replace("-"," ")
aggregatedUserDF["States"] = aggregatedUserDF["States"].str.title()
aggregatedUserDF['States'] = aggregatedUserDF['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


#Map Transaction extraction

path3 = "C:/Users/Admin/Documents/GUVI/phonepe/pulse/data/map/transaction/hover/country/india/state/"
mapTransactionList = os.listdir(path3)

columns3 = {"States" : [],
           "Year" :[],
           "Quater" :[],
           "DistrictName" :[],
           "TransactionCount" : [],
           "TransactionAmount" : []}

for state in mapTransactionList:
    currentState = path3+state+"/"
    mapYearList = os.listdir(currentState)

    for year in mapYearList:
        currentYear = currentState+year+"/"
        mapFileList = os.listdir(currentYear)

        for file in mapFileList:
            currentFile = currentYear+file
            data = open(currentFile,"r")

            loadJson = json.load(data)

            for i in loadJson['data']['hoverDataList']:
                name = i["name"]
                count = i['metric'][0]["count"]
                amount = i['metric'][0]["amount"]
                columns3["DistrictName"].append(name)
                columns3["TransactionCount"].append(count)
                columns3["TransactionAmount"].append(amount)
                columns3["States"].append(state)
                columns3["Year"].append(year)
                columns3["Quater"].append(int(file.strip(".json")))

mapTransactionDF = pd.DataFrame(columns3)

mapTransactionDF["States"] = mapTransactionDF["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
mapTransactionDF["States"] = mapTransactionDF["States"].str.replace("-"," ")
mapTransactionDF["States"] = mapTransactionDF["States"].str.title()
mapTransactionDF['States'] = mapTransactionDF['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#Map User extraction

path4 = "C:/Users/Admin/Documents/GUVI/phonepe/pulse/data/map/user/hover/country/india/state/"
mapUserList = os.listdir(path4)

columns4 = {"States" : [],
               "Year" :[],
               "Quater" :[],
               "DistrictName" :[],
               "RegisteredUsers" :[],
                "AppOpens" : []}

for state in mapUserList:
    currentState = path4+state+"/"
    mapYearList = os.listdir(currentState)

    for year in mapYearList:
        currentYear = currentState+year+"/"
        mapFileList = os.listdir(currentYear)

        for file in mapFileList:
            currentFile = currentYear+file
            data = open(currentFile,"r")

            loadJson = json.load(data)

            for i in loadJson['data']['hoverData'].items():
                district = i[0]
                user = i[1]["registeredUsers"]
                appopen = i[1]["appOpens"]
                columns4["DistrictName"].append(district)
                columns4["RegisteredUsers"].append(user)
                columns4["AppOpens"].append(appopen)
                columns4["States"].append(state)
                columns4["Year"].append(year)
                columns4["Quater"].append(int(file.strip(".json")))

mapUserDF = pd.DataFrame(columns4)

mapUserDF["States"] = mapUserDF["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
mapUserDF["States"] = mapUserDF["States"].str.replace("-"," ")
mapUserDF["States"] = mapUserDF["States"].str.title()
mapUserDF['States'] = mapUserDF['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#Top Transaction extraction

path5 = "C:/Users/Admin/Documents/GUVI/phonepe/pulse/data/top/transaction/country/india/state/"
topTransactionList = os.listdir(path5)

columns5 = {"States" : [],
           "Year" :[],
           "Quater" :[],
           "Pincode" :[],
           "TransactionCount" : [],
           "TransactionAmount" : []}

for state in topTransactionList:
    currentState = path5+state+"/"
    topYearList = os.listdir(currentState)

    for year in topYearList:
        currentYear = currentState+year+"/"
        topFileList = os.listdir(currentYear)

        for file in topFileList:
            currentFile = currentYear+file
            data = open(currentFile,"r")

            loadJson = json.load(data)

            for i in loadJson['data']['pincodes']:
                pin = i["entityName"]
                count = i['metric']["count"]
                amount = i['metric']["amount"]
                columns5["Pincode"].append(pin)
                columns5["TransactionCount"].append(count)
                columns5["TransactionAmount"].append(amount)
                columns5["States"].append(state)
                columns5["Year"].append(year)
                columns5["Quater"].append(int(file.strip(".json")))

topTransactionDF = pd.DataFrame(columns5)

topTransactionDF["States"] = topTransactionDF["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
topTransactionDF["States"] = topTransactionDF["States"].str.replace("-"," ")
topTransactionDF["States"] = topTransactionDF["States"].str.title()
topTransactionDF['States'] = topTransactionDF['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#Top User extraction

path6 = "C:/Users/Admin/Documents/GUVI/phonepe/pulse/data/top/user/country/india/state/"
topUserList = os.listdir(path6)

columns6 = {"States" : [],
           "Year" :[],
           "Quater" :[],
           "Pincode" :[],
           "RegisteredUsersCount" : []}

for state in topUserList:
    currentState = path6+state+"/"
    topYearList = os.listdir(currentState)

    for year in topYearList:
        currentYear = currentState+year+"/"
        topFileList = os.listdir(currentYear)

        for file in topFileList:
            currentFile = currentYear+file
            data = open(currentFile,"r")

            loadJson = json.load(data)

            for i in loadJson['data']['pincodes']:
                pin = i["name"]
                count = i["registeredUsers"]
                columns6["Pincode"].append(pin)
                columns6["RegisteredUsersCount"].append(count)
                columns6["States"].append(state)
                columns6["Year"].append(year)
                columns6["Quater"].append(int(file.strip(".json")))

topUserDF = pd.DataFrame(columns6)

topUserDF["States"] = topUserDF["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
topUserDF["States"] = topUserDF["States"].str.replace("-"," ")
topUserDF["States"] = topUserDF["States"].str.title()
topUserDF['States'] = topUserDF['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


#Creating postgres connection
mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="aslam7862",
            database= "phonepeData",
            port = "5432"
            )
cursor = mydb.cursor()

#Creating aggregated transaction table in postgres
createQuery = '''create table aggregated_transaction(state varchar(80),
                        year int,
                        quater int,
                        transaction_type varchar(30),
                        transaction_count bigint,
                        transaction_amount bigint)'''
cursor.execute(createQuery)
mydb.commit()
        
for index,row in aggregatedTransactionDF.iterrows():
    insertQuery = '''insert into aggregated_transaction(state,
                                                    year,
                                                    quater,
                                                    transaction_type,
                                                    transaction_count,
                                                    transaction_amount)
                                        values(%s,%s,%s,%s,%s,%s)'''
            
    values =(row['States'],
             row['Year'],
             row['Quater'],
             row['TransactionType'],
             row['TransactionCount'],
             row['TransactionAmount'])
    cursor.execute(insertQuery,values)
    mydb.commit()    

#Creating aggregated user table in postgres
createQuery = '''create table aggregated_user(state varchar(80),
                        year int,
                        quater int,
                        brand varchar(30),
                        user_count int,
                        user_percentage float)'''
cursor.execute(createQuery)
mydb.commit()
        
for index,row in aggregatedUserDF.iterrows():
    insertQuery = '''insert into aggregated_user(state,
                                                year,
                                                quater,
                                                brand,
                                                user_count,
                                                user_percentage)
                                        values(%s,%s,%s,%s,%s,%s)'''
            
    values =(row['States'],
             row['Year'],
             row['Quater'],
             row['UserBrand'],
             row['UserCount'],
             row['UserPercentage'])
    cursor.execute(insertQuery,values)
    mydb.commit()

#Creating map transcation table in postgres
createQuery = '''create table map_transaction(state varchar(80),
                        year int,
                        quater int,
                        district_name varchar(50),
                        transaction_count bigint,
                        transaction_amount bigint)'''
cursor.execute(createQuery)
mydb.commit()
        
for index,row in mapTransactionDF.iterrows():
    insertQuery = '''insert into map_transaction(state,
                                                year,
                                                quater,
                                                district_name,
                                                transaction_count,
                                                transaction_amount)
                                        values(%s,%s,%s,%s,%s,%s)'''
            
    values =(row['States'],
             row['Year'],
             row['Quater'],
             row['DistrictName'],
             row['TransactionCount'],
             row['TransactionAmount'])
    cursor.execute(insertQuery,values)
    mydb.commit()

#Creating map user table in postgres
createQuery = '''create table map_user(state varchar(80),
                        year int,
                        quater int,
                        district_name varchar(50),
                        registered_users bigint,
                        app_opens bigint)'''
cursor.execute(createQuery)
mydb.commit()
        
for index,row in mapUserDF.iterrows():
    insertQuery = '''insert into map_user(state,
                                          year,
                                          quater,
                                          district_name,
                                          registered_users,
                                          app_opens)
                                        values(%s,%s,%s,%s,%s,%s)'''
            
    values =(row['States'],
             row['Year'],
             row['Quater'],
             row['DistrictName'],
             row['RegisteredUsers'],
             row['AppOpens'])
    cursor.execute(insertQuery,values)
    mydb.commit()

#Creating top transcation table in postgres
createQuery = '''create table top_transaction(state varchar(80),
                        year int,
                        quater int,
                        pincode int,
                        transaction_count bigint,
                        transaction_amount bigint)'''
cursor.execute(createQuery)
mydb.commit()
        
for index,row in topTransactionDF.iterrows():
    insertQuery = '''insert into top_transaction(state,
                                                year,
                                                quater,
                                                pincode,
                                                transaction_count,
                                                transaction_amount)
                                        values(%s,%s,%s,%s,%s,%s)'''
            
    values =(row['States'],
             row['Year'],
             row['Quater'],
             row['Pincode'],
             row['TransactionCount'],
             row['TransactionAmount'])
    cursor.execute(insertQuery,values)
    mydb.commit()

#Creating top user table in postgres
createQuery = '''create table top_user(state varchar(80),
                        year int,
                        quater int,
                        pincode int,
                        registered_users_count bigint)'''
cursor.execute(createQuery)
mydb.commit()
        
for index,row in topUserDF.iterrows():
    insertQuery = '''insert into top_user(state,
                                          year,
                                          quater,
                                          pincode,
                                          registered_users_count)
                                        values(%s,%s,%s,%s,%s)'''
            
    values =(row['States'],
             row['Year'],
             row['Quater'],
             row['Pincode'],
             row['RegisteredUsersCount'])
    cursor.execute(insertQuery,values)
    mydb.commit()
