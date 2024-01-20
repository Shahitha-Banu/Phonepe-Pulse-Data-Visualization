import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import json
import requests
from streamlit_option_menu import option_menu
from PIL import Image

#Creating postgres connection
mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="aslam7862",
            database= "phonepeData",
            port = "5432"
            )
cursor = mydb.cursor()

#Aggregated_transsaction
cursor.execute("select * from aggregated_transaction")
mydb.commit()
table1 = cursor.fetchall()
aggregatedTransactionDF = pd.DataFrame(table1,columns = ("States", "Years", "Quarter", "TransactionType", "TransactionCount", "TransactionAmount"))

#Aggregated_user
cursor.execute("select * from aggregated_user")
mydb.commit()
table2 = cursor.fetchall()
aggregatedUserDF = pd.DataFrame(table2,columns = ("States", "Years", "Quarter", "Brands", "UserCount", "UserPercentage"))

#Map_transaction
cursor.execute("select * from map_transaction")
mydb.commit()
table3 = cursor.fetchall()
mapTransactionDF = pd.DataFrame(table3,columns = ("States", "Years", "Quarter", "Districts", "TransactionCount", "TransactionAmount"))

#Map_user
cursor.execute("select * from map_user")
mydb.commit()
table4 = cursor.fetchall()
mapUserDF = pd.DataFrame(table4,columns = ("States", "Years", "Quarter", "Districts", "RegisteredUser", "AppOpens"))

#Top_transaction
cursor.execute("select * from top_transaction")
mydb.commit()
table5 = cursor.fetchall()
topTransactionDF = pd.DataFrame(table5,columns = ("States", "Years", "Quarter", "Pincodes", "TransactionCount", "TransactionAmount"))

#Top_user
cursor.execute("select * from top_user")
mydb.commit()
table6 = cursor.fetchall()
topUserDF = pd.DataFrame(table6, columns = ("States", "Years", "Quarter", "Pincodes", "RegisteredUserCount"))

#Ploting based on Transaction Type
def plotAggregatedTransactionType(typeT,year,quater):
    df1 = aggregatedTransactionDF[aggregatedTransactionDF["TransactionType"] == typeT]
    df1.reset_index(drop= True, inplace= True)

    df1 = df1[df1["Years"] == year]
    df1.reset_index(drop= True, inplace= True)

    df1 = df1[df1["Quarter"] == quater]
    df1.reset_index(drop= True, inplace= True)

    tab1, tab2 = st.tabs ([":blue[Transaction Amount]",":blue[Transaction Count]"])
    with tab1:

        fig1= px.bar(df1, x="States", y= "TransactionAmount",title= f"Aggregated TRANSACTION AMOUNT for {year} in quater {quater} of type {typeT}",
                           width=900, height= 650, color_continuous_scale =px.colors.sequential.Plotly3 ,color = "TransactionAmount")
        st.plotly_chart(fig1)
    with tab2:

        fig2= px.bar(df1, x="States", y= "TransactionCount",title= f"Aggregated TRANSACTION COUNT for {year} in quater {quater} of type {typeT}",
                           width=900, height= 650, color_continuous_scale =px.colors.sequential.Agsunset, color = "TransactionCount")
        st.plotly_chart(fig2)

#plot based on Brand
def plotAggregatedBrandType(model):
    
    df2 = aggregatedUserDF[aggregatedUserDF["Brands"] == model]
    df2.reset_index(drop= True, inplace= True)
    
    df2_count = df2.groupby(["States","Years"])[["UserCount"]].sum()
    df2_count.reset_index(inplace= True)

    df2_percent = df2.groupby(["States","Years"])[["UserPercentage"]].sum()
    df2_percent = df2_percent.groupby("States")[["UserPercentage"]].sum()
    df2_percent.reset_index(inplace= True)

    tab1, tab2 = st.tabs ([":blue[User Count Brand-Wise]",":blue[Percentage of User]"])
    with tab1:

        fig1= px.area(df2_count, x="States", y= "UserCount",title= f"Aggregated USER COUNT for the {model} model",
                           width=800, height= 700, color_discrete_sequence=px.colors.diverging.PiYG, color = "UserCount", line_group = "UserCount")
        st.plotly_chart(fig1)
    with tab2:

        fig2= px.pie(df2_percent, names="States", values= "UserPercentage",title= f"Aggregated User Percentage for the {model} model",
                           width=800, height= 900, color_discrete_sequence=px.colors.diverging.Portland,hole = 0.3, color = "UserPercentage")
        st.plotly_chart(fig2)

#Plotting District wise transaction data
def plotDiscrictTransaction(state, district):
    
    district = district + "district"
    district = district.lower()
   
    df3 = mapTransactionDF[mapTransactionDF["States"] == state]
    df3.reset_index(drop= True, inplace= True)
    
    df3 = df3[df3["Districts"] == district]
    df3.reset_index(drop= True, inplace= True)

    df3_count = df3.groupby("Years")[["TransactionCount"]].sum()
    df3_count.reset_index(inplace= True)
    
    df3_amount = df3.groupby("Years")[["TransactionAmount"]].sum()
    df3_amount.reset_index(inplace= True)

    col1, col2 = st.columns(2)
    with col1:

        fig1= px.pie(df3_count, names="Years", values= "TransactionCount",title= f"TRANSACTION COUNT for {district}",
                           width=350, height= 400, color_discrete_sequence= px.colors.cyclical.HSV,hole = 0.1, color = "TransactionCount")
        st.plotly_chart(fig1)

    with col2:
        fig2= px.pie(df3_amount, names="Years", values= "TransactionAmount",title= f"TRANSACTION AMOUNT for {district}",
                           width=350, height= 400, color_discrete_sequence = px.colors.cyclical.mrybm, hole = 0.1,color = "TransactionAmount")
        st.plotly_chart(fig2)

#Plotting District wise user data
def plotDiscrictUser(state):

    df4 = mapUserDF[mapUserDF["States"] == state]
    df4.reset_index(drop = True,inplace = True)

    df4_user = df4.groupby(["Years", "Districts"])[["RegisteredUser"]].sum()
    df4_user.reset_index(inplace = True)
    
    df4_user = df4_user.groupby("Districts")[["RegisteredUser"]].sum()
    df4_user.reset_index(inplace = True)
    
    df4_appopen = df4.groupby("Districts")[["AppOpens"]].sum()
    df4_appopen.reset_index(inplace = True)
        
    tab1, tab2 = st.tabs([":blue[Registered USER District-Wise]", ":blue[App onens in year-wise in all districts]"])
    with tab1:

        fig1= px.funnel(df4_user, x="Districts", y="RegisteredUser",title= f"Registered number of user in {state} district-wise",
                           width=600, height= 700, color_discrete_sequence = px.colors.diverging.Spectral, color = "RegisteredUser")
        st.plotly_chart(fig1)

    with tab2:
        fig2= px.sunburst(df4_appopen, path = ["Districts","AppOpens"], values= "AppOpens",
                          title= f"Number of APP opens in {state} disctrict-wise",
                          width=700, height= 750, color_continuous_scale = px.colors.cyclical.Edge, color = "AppOpens")
                          
        st.plotly_chart(fig2)

#Plotting Analysis question 1
def plotQuestion1():
    
    df5 = aggregatedUserDF.groupby("Brands")[["UserCount"]].sum()
    df5.reset_index(inplace = True)
    
    df5 = df5.sort_values(by = ["UserCount"],ascending=False)
    df5.reset_index(drop = True, inplace = True)
    
    fig1= px.bar(df5[0:5], x="Brands", y= "UserCount",title= "Top 5 brands of mobile",
                           width=600, height= 450, color_discrete_sequence = px.colors.sequential.Cividis, color = "UserCount")
    st.plotly_chart(fig1)
    
    
#Plotting Analysis question 2
def plotQuestion2():
    
    df6 = aggregatedTransactionDF.groupby("States")[["TransactionCount"]].sum()
    df6.reset_index(inplace = True)
    
    df6 = df6.sort_values(by = ["TransactionCount"],ascending=False)
    df6.reset_index(drop = True, inplace = True)
    
    fig1= px.line(df6[0:10], x="States", y= "TransactionCount",title= "Top 10 states with highest transaction count",
                               width=600, height= 450, color_discrete_sequence=px.colors.diverging.curl, symbol = "TransactionCount")
    st.plotly_chart(fig1)

#Plotting Analysis question 3
def plotQuestion3():
    
    df7 = mapUserDF.groupby("Districts")[["RegisteredUser"]].sum()
    df7.reset_index(inplace = True)
    
    df7 = df7.sort_values(by = ["RegisteredUser"],ascending = False)
    df7.reset_index(drop = True, inplace = True)
    
    fig1= px.funnel(df7[0:25], x="Districts", y= "RegisteredUser",title= "Top 25 districts with highest registered user",
                           width=650, height= 550, color_discrete_sequence = px.colors.sequential.Plasma, color = "RegisteredUser")
    st.plotly_chart(fig1)  

#Plotting Analysis question 4
def plotQuestion4():
    
    dict1 = {"States" : [], "Pincode" : [], "TransactionAmount" : []}
    df8 = topTransactionDF.groupby(["States","Pincodes"])[["TransactionAmount"]].sum()
    df8.reset_index(inplace = True)
    
    df8 = df8.sort_values(by = ["TransactionAmount"], ascending = False)
    df8.reset_index(drop = True,inplace = True)
    
    for index,row in df8.iterrows():
        if row["States"] not in dict1["States"]:
            dict1["States"].append(row["States"])
            dict1["Pincode"].append(row["Pincodes"])
            dict1["TransactionAmount"].append(row["TransactionAmount"])
    df8_new = pd.DataFrame(dict1)
    
    fig1= px.bar(df8_new, x="States", y= "TransactionAmount",title= "Highest transaction amount of each state pincode-wise",
                           width=700, height= 550, color_continuous_scale = px.colors.sequential.Inferno, color = "Pincode")
    st.plotly_chart(fig1)        
        
#Plotting Analysis question 5
def plotQuestion5():
    
    df9 = topUserDF.groupby(["States","Pincodes"])[["RegisteredUserCount"]].sum()
    df9.reset_index(inplace = True)
    
    df9 = df9.sort_values(by = ["RegisteredUserCount"],ascending = False)
    df9.reset_index(drop = True, inplace = True)
    
    fig1= px.sunburst(df9[0:10], path = ["States", "Pincodes"], values= "RegisteredUserCount",
                          title= "Pincode based top 10 registered user",
                          width=700, height= 450, color_continuous_scale = px.colors.cyclical.Phase, color = "RegisteredUserCount")
    st.plotly_chart(fig1)  
    
#Plotting Analysis question 6
def plotQuestion6():

    df10 = topUserDF.groupby(["States","Pincodes"])[["RegisteredUserCount"]].sum()
    df10.reset_index(inplace = True)
    
    df10 = df10.sort_values(by = ["RegisteredUserCount"])
    df10.reset_index(drop = True, inplace = True)
    
    fig1= px.sunburst(df10[0:10], path = ["States", "Pincodes"], values= "RegisteredUserCount",
                          title= "Pincode based top 10 registered user",
                          width=700, height= 450, color_continuous_scale = px.colors.cyclical.HSV, color = "RegisteredUserCount")
    st.plotly_chart(fig1)  
    
#Plotting Analysis question 7
def plotQuestion7():

    df11 = aggregatedTransactionDF.groupby("States")[["TransactionCount"]].sum()
    df11.reset_index(inplace = True)
    
    df11 = df11.sort_values(by = ["TransactionCount"])
    df11.reset_index(drop = True, inplace = True)
    
    fig1= px.histogram(df11[0:10], x="States", y= "TransactionCount",title= "Least 10 states with transaction count",
                           width=600, height= 450, color_discrete_sequence=px.colors.sequential.Turbo, color = "TransactionCount")
    st.plotly_chart(fig1)
    
    
#streamlit designing part
str1 = """ :purple[The goal is to extract the Phonepe pulse Github repository which
                contains a large amount of data related to
                various metrics and statistical data and process it to obtain
                insights and information that can be visualized in a user-friendly manner.]"""
img1= Image.open(r"C:\Users\Admin\Documents\GUVI\phone_pe.jpg")

with st.sidebar:
    st.image(img1, width=200)
    st.header(":orange[About]")

    with st.container():
        st.caption(str1)
st.subheader(":green[Phonepe Pulse Data Visualization and Exploration]",divider = "rainbow")

selectedMenu = option_menu(None, ["Dashboard", "Metrics", "Insights", 'Analysis'], 
                icons=['bar-chart', 'pie-chart', "geo", 'align-top'], 
                menu_icon="cast", default_index=0, orientation="horizontal")

#Aggregated Transaction plot of amount and count
if selectedMenu == "Dashboard":
    
    col1, col2 = st.columns([0.55,0.45])
    with col1:
        st.subheader(":rainbow[Geo-Choropleth Plot]", divider = "violet")   
        metricDF = mapTransactionDF.groupby(["States"])[["TransactionCount", "TransactionAmount"]].sum()
        metricDF.reset_index(inplace = True)
        
        url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response= requests.get(url)
        mapData= json.loads(response.content)
        statesName = [feature["properties"]["ST_NM"] for feature in mapData["features"]]
        statesName.sort()
                     
        fig1 = px.choropleth(metricDF, geojson= mapData, locations= "States", featureidkey= "properties.ST_NM",
                                     color= "TransactionAmount", color_continuous_scale= px.colors.sequential.Purp,
                                     hover_name= "States",title = "TRANSACTION AMOUNT Geo-choropleth", fitbounds= "locations",
                                     width =400, height= 350)
        fig1.update_geos(visible =False)
        st.plotly_chart(fig1)          
        
        fig2 = px.choropleth(metricDF, geojson= mapData, locations= "States", featureidkey= "properties.ST_NM",
                                     color= "TransactionCount",color_continuous_scale= px.colors.sequential.RdPu,
                                     hover_name= "States",title = "TRANSACTION COUNT Geo-choropleth",
                                     fitbounds= "geojson", width =400, height=350)
        fig2.update_geos(visible =False)
        st.plotly_chart(fig2)

    with col2:
        st.subheader(":rainbow[Interesting Facts]", divider = "violet")
        table1 = aggregatedTransactionDF.groupby(["TransactionType"]) [["TransactionCount"]].sum()
        table1.reset_index(inplace = True)

        st.caption(":blue[Transaction Type Based]")
        st.dataframe(table1, hide_index = "True")

        table2 = aggregatedUserDF.groupby("Brands")[["UserCount"]].sum()
        table2.reset_index(inplace = True)

        st.caption(":blue[Brand Model Based]")
        st.dataframe(table2, hide_index = "True")
        
elif selectedMenu == "Metrics":
    tab1, tab2 = st.tabs([":blue[Based on Transaction Type]", ":blue[Based on Brand]"])
    with tab1:
        col1, col2,col3 = st.columns(3)
        with col1:
            transactionType = st.selectbox(":blue[Select a transaction type]",aggregatedTransactionDF["TransactionType"].unique(),index = None, placeholder = "Select one option")
                            
        with col2:
            if transactionType != None:
                transactionYear = st.selectbox(":blue[Select the year]",aggregatedTransactionDF["Years"].unique())

        with col3:
            if transactionType != None:                
                if transactionYear != 2023:
                    transactionQuater = st.selectbox(":blue[Select the Quarter]",[1,2,3,4],key = 1)
                else:
                    transactionQuater = st.selectbox(":blue[Select the Quarter]",[1,2,3], key = 1)

        if transactionType != None:
            plotAggregatedTransactionType(transactionType,transactionYear,transactionQuater)

    with tab2:

        brandModel = st.selectbox(":blue[Select a brand model]",aggregatedUserDF["Brands"].unique(),index = None, placeholder = "Select one option")

        if brandModel != None:
            plotAggregatedBrandType(brandModel)

elif selectedMenu == "Insights":
    plotOption = st.radio(":blue[Select the option]",[":red[Transaction Based Plot]",":red[User Based Plot]"])
    if plotOption == ":red[Transaction Based Plot]":
        col1, col2 = st.columns(2)

        with col1:
            transactionState = st.selectbox(":blue[Select the state]",aggregatedTransactionDF["States"].unique(),index = None, placeholder = "Select one option")

        with col2:
            if transactionState != None:
                transactionDistrict = mapTransactionDF[mapTransactionDF["States"] == transactionState]
                transactionDistrict["Districts"] = transactionDistrict["Districts"].str.replace("district","")
                transactionDistrict["Districts"] = transactionDistrict["Districts"].str.title()
                transactionDistrict = st.selectbox(":blue[Select the district]",transactionDistrict["Districts"].unique(),index = None,
                                                       placeholder = "Select the state first")
                
        if transactionState != None and transactionDistrict != None:
                plotDiscrictTransaction(transactionState,transactionDistrict)

    else:
        transactionState = st.selectbox(":blue[Select the state]",aggregatedTransactionDF["States"].unique(),index = None, placeholder = "Select one option")
        if transactionState != None:
            plotDiscrictUser(transactionState)

elif selectedMenu == "Analysis":
    questions = ["Top 5 brands of mobile",
                 "Top 10 states with highest transaction count",
                 "Top 25 districts with highest registered user",
                 "Highest transaction amount of each state pincode-wise",
                 "Pincode based top 10 registered user",
                 "Pincode based least 10 registered user",
                 "Least 10 states with transaction count"]

    plotQuestion = st.selectbox(":blue[Select the plotting creteria]",questions,index = None,placeholder = "Select one option")

    if plotQuestion == "Top 5 brands of mobile":
        plotQuestion1()

    elif plotQuestion == "Top 10 states with highest transaction count":
        plotQuestion2()

    elif plotQuestion == "Top 25 districts with highest registered user":
        plotQuestion3()

    elif plotQuestion == "Highest transaction amount of each state pincode-wise":
        plotQuestion4()

    elif plotQuestion == "Pincode based top 10 registered user":
        plotQuestion5()

    elif plotQuestion == "Pincode based least 10 registered user":
        plotQuestion6()
        
    elif plotQuestion == "Least 10 states with transaction count":
        plotQuestion7()
