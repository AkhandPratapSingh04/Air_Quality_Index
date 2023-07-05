import requests
import sys
import os   
import pandas as pd 
from bs4 import BeautifulSoup
import csv
import glob


def meta_data(month,year):
    file_html=open('data/html_data/{}/{}.html'.format(year,month),'rb')
    plain_text=file_html.read()

    tempD=[]
    finalD=[]

    soup=BeautifulSoup(plain_text,"lxml")
    for table in soup.findAll('table', {'class':'medias mensuales numspan'}):
        #appending all text from tbody
        for tbody in table :
            for tr in tbody:
                a=tr.get_text()
                #appending all values , each and every row
                tempD.append(a)

    rows=len(tempD)/15

    for times in range(round(rows)):
        newtempD=[]
        for i in range(15):
            newtempD.append(tempD[0])
            tempD.pop(0)
        finalD.append(newtempD)

    length=len(finalD)
    finalD.pop(length-1)
    finalD.pop(0)

    for a in range(len(finalD)):
        finalD[a].pop(6)
        finalD[a].pop(13)
        finalD[a].pop(12)
        finalD[a].pop(11)
        finalD[a].pop(10)
        finalD[a].pop(9)
        finalD[a].pop(0)

    return finalD

def data_combine(year,cs):
    for a in pd.read_csv('data/Html_main_data/real_'+str(year)+".csv",chunksize=cs):
        df=pd.DataFrame(data=a)
        mylist=df.values.tolist()
    return mylist



if __name__=="__main__" :
    if not os.path.exists("data/Html_main_data"):
        os.makedirs("data/Html_main_data")
    for year in range(2014,2023):
        final_data=[]
        with open('data/Html_main_data/real_'+str(year)+".csv",'w') as csvfile:
            wr=csv.writer(csvfile,dialect= 'excel')
            wr.writerow(
                ['T','TM','Tm','SLP','H','VV','V','VM'])
        for month in range(1,13):
            temp=meta_data(month,year)
            final_data=final_data+temp
        
        with open('data/Html_main_data/real_'+str(year)+".csv",'a') as csvfile:
            wr=csv.writer(csvfile,dialect='excel')
            for row in final_data:
                flag=0
                for e in row :
                    if e=='' or e=="-" :
                        flag=1
                if flag !=1:
                    wr.writerow(row)
    data_2014=data_combine(2014,600)
    data_2015=data_combine(2015,600)
    data_2016=data_combine(2016,600)
    data_2017=data_combine(2017,600)
    data_2018=data_combine(2018,600)
    data_2019=data_combine(2015,600)
    data_2020=data_combine(2020,600)
    data_2021=data_combine(2021,600)
    data_2022=data_combine(2022,600)

    total=data_2014+data_2015+data_2016+data_2017+data_2018+data_2019+data_2020+data_2021+data_2022


    with open('data/Html_main_data/final_combine.csv','w') as csvfile:
        wr=csv.writer(csvfile,dialect='excel')
        wr.writerow(
            ['T','TM','Tm','SLP','H','VV','V','VM']
        )
        wr.writerows(total)
            



