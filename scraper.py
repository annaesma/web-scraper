# -*- coding: utf-8 -*-
"""
Created on Fri Dec 13 13:04:59 2019

@author: dzakmic esma
"""
# following program parses an url and extracts data into MySQL database
pip install mysql-connector-python==8.0.11
pip install mysqlclient
pip install schedule
pip install time
import schedule
import time
from datetime import datetime,timedelta
import mysql.connector
from mysql.connector import Error
mydb=mysql.connector.connect(host="localhost",user="root",passwd="Anna0402")
mycursor=mydb.cursor(buffered=True)
mycursor=mydb.cursor(prepared=True)
mycursor.execute("create database call_transcripts")
mycursor.execute("use call_transcripts")
mycursor.execute("create table if not exists call_transcipts ( id int not null auto_increment,document_url varchar(100), document_title text, issue_time timestamp, insert_time timestamp, document_body text, document_page int,document_subtitle text,company_ticker varchar(5),market varchar(10),primary key(id))") 

from bs4 import BeautifulSoup as bs
url_page=('https://seekingalpha.com/earnings/earnings-call-transcripts') #main page

from urllib.request import Request, urlopen
req = Request(url_page, headers={'User-Agent': 'Mozilla/5.0'}) # in order to prevent error 403

webpage = urlopen(req).read()
webpage_soup=bs(webpage,"html.parser")
containers=webpage_soup.findAll("li",{"class":"list-group-item article"})
#make a function to loop over links and extract the following:
ticker=[] 
market=[]
doc_url=[] 
doc_title=[] 
doc_date=[] 
insert_date=[] 
doc_body=[]
page=[]
page_position=[]
doc_subtitle=[]
link_webpage=[]
urls1 = [link["href"] for link in webpage_soup.find_all("a", href=True) if link["href"].startswith("/article/") | link["href"].endswith("transcript")] # urls to idividual records

def scraper(): # wrap everything into a function to schedule it for daily run
    for url in urls1:
        link_webpage.append('https://seekingalpha.com/' + url)
    for link in link_webpage: 
        req_link=Request(link, headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.3'})
        link_open=urlopen(req_link).read()
        link_soup=bs(link_open,"html.parser")
        link_containers=link_soup.findAll('h1')
        doc_title.append(link_containers.text)
        pages=link_soup.findAll("div",{"span":"pages"})
        doc_page.append(pages.text)
        time=link_soup.find_all("div",{"class":"a-info clearfix"})
        doc_time.append(time.time)
        body=link_soup.find("div","class":"sa-art article-width")
        doc_body.append(body.select('id="a-body"'))
        
        
        
    
     
    for container in containers:
       
       
       linkk=container.findAll("a",{"class":"dashboard-article-link"})
       ticker.append(container.div.select('a')[0].text) 
       insert_date.append(datetime.now()) 
       tet=webpage_soup.find("h3",{"class":"list-group-item-heading"})
       doc_title.append(tet.text)
       new_url=linkk.href
    
# save into csv then load into mysql
    rows = zip(insert_date,ticker)
    import csv

    with open("call_transcripts", "w") as f:
        
         writer = csv.writer(f)
         for row in rows:
             writer.writerow(row)
               
               
     with open('call_transcripts1') as csv_file: # load into mysql not succesefull   
          
        csv_reader = csv.reader(csv_file, delimiter=',')
     for row in csv_reader:
         mycursor.execute('use call_transcripts')
         mycursor.execute('INSERT INTO nova12(insert_date,ticker) values ("%dt","%s")',row)
     mydb.commit()
               
import schedule #schedule function scrapper to run every day
import time
schedule.every().day.at("13:00").do(scraper) 
while 1:
    schedule.run_pending()
    time.sleep(1)
                
ticker=["s","e"]
mycursor.execute("create table nova_table (ticker int)")
query="""insert into nova_table (ticker) values ("%S")"""
mycursor.execute(query,ticker)
mycursor.execute("INSERT INTO nova_table ticker VALUES ('{}') ".format(ticker)) 
