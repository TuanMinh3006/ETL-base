# -*- coding: utf-8 -*-
"""
Created on Sun Feb 25 23:03:17 2024

@author: ADMIN
"""
import pandas as pd 
import glob
import xml.etree.ElementTree as ET
from datetime import datetime

target_file=r"D:\Python\ETL base\transformed_data.csv"
log_file=r"D:\Python\ETL base\log_file.txt"

def extract_from_csv(file_csv):
    dataframe = pd.read_csv(file_csv)
    return dataframe

def extract_from_json(file_json):
    dataframe = pd.read_json(file_json,lines=True)
    return dataframe

def extract_from_xml(file_xml):
    dataframe =pd.DataFrame(columns=["car_model",'year_of_manufacture','price','fuel'])
    tree=ET.parse(file_xml)
    root=tree.getroot()
    for car in root:
        model=car.find('car_model').text
        year_of_manufacture=int(car.find('year_of_manufacture').text)
        price=float(car.find('price').text)
        fuel=car.find('fuel').text
        dataframe=pd.concat([dataframe,pd.DataFrame([{"car_model":model,'year_of_manufacture':year_of_manufacture,'price':price,'fuel':fuel}])])
    return dataframe
def extract():
    extracted_data=pd.DataFrame(columns=["car_model",'year_of_manufacture','price','fuel'])
    
    for csv_file in glob.glob("D:\Python\ETL base\datasource\*.csv"):
        extracted_data=pd.concat([extracted_data,pd.DataFrame(extract_from_csv(csv_file))],ignore_index=True)
    
    for json_file in glob.glob("D:\Python\ETL base\datasource\*.json"):
        extracted_data=pd.concat([extracted_data,pd.DataFrame(extract_from_json(json_file))],ignore_index=True)
    
    for xml_file in glob.glob("D:\Python\ETL base\datasource\*.xml"):
        extracted_data=pd.concat([extracted_data,pd.DataFrame(extract_from_xml(xml_file))],ignore_index=True)
        
    return extracted_data
extracted_data=extract()
print(extracted_data)

def transform(data):
    data["price"]=round(data.price,2)
    return data
    
def load(target_file,data):
    data.to_csv(target_file)

def log_progress(message):
    timestamp_format='%Y-%h-%d-%H:%M:%S' #Year -Monthname-day_Hour-minute-second
    now =datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp+","+ message +"\n")

log_progress(" ETL JOB STARTED")

log_progress("EXTRACT PRASE STARTED")
extracted_data=extract()
log_progress("EXTRACT PRASE END")

log_progress("TRANSFORM PRASE STARTED")
transformed_data=transform(extracted_data)
log_progress("TRANSFORM PRASE END")
log_progress("LOADING PRASE STARTED")
load(target_file,transformed_data)
log_progress("LOADING PRASE END")
