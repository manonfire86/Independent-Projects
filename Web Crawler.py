# -*- coding: utf-8 -*-
"""
Created on Thu May 24 12:58:44 2018

@author: santana
"""
from lxml import html
import requests
import unicodecsv as csv
import argparse
from pyzillow.pyzillow import ZillowWrapper,GetDeepSearchResults
import pandas as pd
from bs4 import BeautifulSoup
import re
import time as t
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import os
from dateutil.rrule import *
import datetime as dt
from dateutil.parser import *

os.chdir(r'\\Tolisfile01\public\Work\Technology\Python Tools - SOP, Scripts, Data\Zillow')

### Parse Files

zipdb = pd.read_excel(r'\\Tolisfile01\public\Work\Technology\Python Tools - SOP, Scripts, Data\Zillow\Zipcode Database.xlsx')

pipedf = pd.read_excel(r'\\Tolisfile01\public\Work\Technology\Python Tools - SOP, Scripts, Data\Zillow\Pipeline Inventory_June212018.xlsx')
pipepaddress = pipedf[pipedf['ZipCode']==zipdb['Zipcode'][0]]['Address']

### Webcrawl

address_headers = {
        'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
        }

properties_list = []
for i in range(len(zipdb)):
    t.sleep(7)
    adrequest = requests.get("https://www.zillow.com/homes/for_sale/{0}_rb/?fromHomePage=true&shouldFireSellPageImplicitClaimGA=false&fromHomePageTab=buy".format(zipdb['Zipcode'][0]),headers=address_headers)
    adcont = html.fromstring(adrequest.content)
    reflink = adcont.xpath('//*[@id="search-pagination-wrapper"]/ol/li[8]/a[text()]')[0].get('href')
    numberofpages = int(adcont.xpath('//*[@id="search-pagination-wrapper"]//text()')[-2])
    for j in range(1,numberofpages):
        finaladrequestlink = requests.get('https://www.zillow.com/homes/for_sale/{0}_rb/'.format(zipdb['Zipcode'][i])+str(j)+'_p/',headers=address_headers)
        adcont2 = html.fromstring(finaladrequestlink.content)
        search_results = adcont2.xpath("//div[@id='search-results']//article")
        for properties in search_results:
            raw_address = properties.xpath(".//span[@itemprop='address']//span[@itemprop='streetAddress']//text()")
            address = ' '.join(' '.join(raw_address).split()) if raw_address else None
            properties = {'address':address}
            properties_list.append(properties)


addressdf = pd.DataFrame.from_dict(properties_list)

ajheaders= {
        'accept':'*/*',
        'accept-encoding':'gzip, deflate, br',
        'accept-language':'en-US,en;q=0.9',
        'cache-control':'max-age=0',
        'upgrade-insecure-requests':'1',
        'user-agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36'
		}

masterparsedf = {}
for i in range(len(addressdf['address'])):
    t.sleep(13)
    r = requests.get("https://www.zillow.com/homes/for_sale/{0}".format(re.sub(' ','-',addressdf['address'][i]))+',-'+str(zipdb['Zipcode'][0])+'_rb/',headers=ajheaders)
    ajurl = re.findall(re.escape('AjaxRender.htm?')+'(.*?)"',r.content)
    if len(ajurl)>0:
        aurl = "https://www.zillow.com/AjaxRender.htm?{}".format(ajurl[3])
        r = requests.get(aurl,headers=ajheaders)
        soup = BeautifulSoup(r.content.replace('\\',''),"html.parser")
        tabledat = []
        for tr in soup.find_all('tr'):
           tabledat.append([td.text for td in tr.find_all('td')])
        masterparsedf[addressdf['address'][i]] = pd.DataFrame(tabledat)
    else:
        continue


masterparsedf2 = masterparsedf

dateind = list(rrule(DAILY,dtstart=parse("01/01/2000"),until= dt.datetime.today()))
datetimeindlist = []
for i in dateind:
    datetimeindlist.append(i.strftime('%m/%d/%y'))
    
completedf = pd.DataFrame(index = datetimeindlist)
dflisttt = []
headerlist = []
for i in range(len(masterparsedf)):
    if len(masterparsedf.values()[i]) == 0:
        continue
    else:
        completedf = pd.merge(completedf,pd.DataFrame(masterparsedf2.values()[i][2]).set_index(masterparsedf2.values()[i][0]),how='left',left_index=True,right_index=True)
        headerlist.append(masterparsedf.keys()[i])

completedf.columns = headerlist

completedf = completedf.T

cleandf = completedf.dropna(axis='columns',how='all')
cleandf = cleandf.fillna(0)
cleandf = cleandf.T



colparsed = set(list(cleandf.columns.values))
cleanaddresses = addressdf.drop_duplicates(keep='first')

unparseddata = []
for i in range(len(addressdf)):
    if addressdf['address'][i] in colparsed:
        continue
    else:
        unparseddata.append(addressdf['address'][i])

unparseddf = pd.DataFrame(unparseddata,columns=['Unparsed Addresses'])

### Address, URL

addurl = {}
for i in range(len(addressdf['address'])):
    addurl[addressdf['address'][i]] = "https://www.zillow.com/homes/for_sale/{0}".format(re.sub(' ','-',addressdf['address'][i]))+',-'+str(zipdb['Zipcode'][0])+'_rb/'

urldf = pd.DataFrame.from_dict(addurl,orient='index')
urldf.columns = ['URL']

addunparurl = {}
for i in range(len(unparseddf['Unparsed Addresses'])):
    addunparurl[unparseddf['Unparsed Addresses'][i]] = "https://www.zillow.com/homes/for_sale/{0}".format(re.sub(' ','-',unparseddf['Unparsed Addresses'][i]))+',-'+str(zipdb['Zipcode'][0])+'_rb/'

unparurldf = pd.DataFrame.from_dict(addunparurl,orient='index')
unparurldf.columns = ['URL']


### Event Table

eventcompletedf = pd.DataFrame(index = datetimeindlist)
eventdflisttt = []
eventheaderlist = []
for i in range(len(masterparsedf)):
    if len(masterparsedf.values()[i]) == 0:
        continue
    else:
        eventcompletedf = pd.merge(eventcompletedf,pd.DataFrame(masterparsedf2.values()[i][1]).set_index(masterparsedf2.values()[i][0]),how='left',left_index=True,right_index=True)
        eventheaderlist.append(masterparsedf.keys()[i])

eventcompletedf.columns = eventheaderlist

eventcompletedf = eventcompletedf.T

eventcleandf = eventcompletedf.dropna(axis='columns',how='all')
eventcleandf = eventcleandf.fillna(0)
eventcleandf = eventcleandf.T


### Pipeline

pipeaddressfin = []
for i in pipepaddress:
    pipeaddressfin.append(i.split(',')[1]+' '+i.split(',')[0])

strpipadd = []
for i in pipeaddressfin:
    strpipadd.append(str(i))

mappeddf = cleandf.T
mappeddf = mappeddf.loc[mappeddf.index.isin(map(str.strip,strpipadd))].T

eventmappeddf = eventcleandf.T
eventmappeddf = eventmappeddf.loc[eventmappeddf.index.isin(map(str.strip,strpipadd))].T


## RE Comps File

Excelwriter = pd.ExcelWriter('RE Comps Analysis '+dt.datetime.today().strftime('%m-%d-%Y')+'.xlsx')
cleandf.to_excel(Excelwriter,'Price History')
urldf.to_excel(Excelwriter,'Addresses Parsed')
unparurldf.to_excel(Excelwriter,'Unparsed Addresses')
eventcleandf.to_excel(Excelwriter,'Event History')
Excelwriter.save()
Excelwriter.close()

### Mapping File

Excelwritertwo = pd.ExcelWriter('Mapped Pipeline '+dt.datetime.today().strftime('%m-%d-%Y')+'.xlsx')
mappeddf.to_excel(Excelwritertwo,'Mapped Price History')
eventmappeddf.to_excel(Excelwritertwo,'Mapped Event History')
Excelwritertwo.save()
Excelwritertwo.close()

