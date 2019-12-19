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
pip install selenium

import schedule
import time
from datetime import datetime,timedelta
import mysql.connector
from mysql.connector import Error
mydb=mysql.connector.connect(host="localhost",user="root",passwd="Anna0402")
mycursor=mydb.cursor(buffered=True)
mycursor.execute("create database call_transcripts")
mycursor.execute("use call_transcripts")
mycursor.execute("create table if not exists transcript_documents ( id int not null auto_increment,website_link varchar(100), title text,subtitle issue_time timestamp, insert_time timestamp,body text, website_page int,page_position,company_ticker varchar(5),market varchar(10),primary key(id))") 


#make a function to loop over links and extract the following:
ticker=[] 
title=[] 
insert_date=[]
link_webpage=[]
doc_date=[] # problematican nema sve unose
doc_subtitle=[] # almost there

doc_body=[] #almost there selenium blocked
doc_page=[] #almost there selenium blocked

market=[]
page_position=[]

def scraper():
    from bs4 import BeautifulSoup as bs
    from selenium import webdriver
    import schedule
    import time
    from datetime import datetime,timedelta
    import mysql.connector
    from mysql.connector import Error
    mydb=mysql.connector.connect(host="localhost",user="root",passwd="Anna0402")
    mycursor.execute("use call_transcripts")
    
    url_page=('https://seekingalpha.com/earnings/earnings-call-transcripts') #main page

    driver=webdriver.Chrome("C:/Users/dzakmice/chromedriver.exe")
    driver.get(url_page)
    content=driver.page_source
    webpage_soup=bs(content)# wrap everything into a function to schedule it for daily run
    urls1 = [link["href"] for link in webpage_soup.find_all("a", href=True) if link["href"].startswith("/article/") | link["href"].endswith("transcript")] # urls to idividual records
    for url in urls1:
        link_webpage.append('https://seekingalpha.com/' + url)
    for link in link_webpage:    
        driver.get(link)
        content_link=driver.page_source
        link_soup=bs(link_content)
    

    for l_soup in link_soup:    
       
    
        doc_subtitle=l_soup.find("p",{"class":"p p1"})

        body=link_soup.find("div",{"class":"sa-art article-width"})
        doc_body.append(body.select('id="a-body"'))
        pages=link_soup.findAll("div",{"span":"pages"})
        doc_page.append(pages.text)
        doc_subtitle=soup.find_all("p",{"class":"p p1"})[0].text # awesome!


        time=link_soup.find_all("div",{"class":"a-info clearfix"})
        doc_time.append(time.time)
        
    containers=webpage_soup.findAll("li",{"class":"list-group-item article"})
    for container in containers:
       
       ticker.append(container.div.select('a')[0].text) 
       title.append(container.select('a')[0].text)
       insert_date.append(datetime.now()) 
       tet=webpage_soup.find("div",{"class":"article-desc"})
       doc_date.append(tet.text[13:25])
        
    
# save into csv then load into mysql
    rows = zip(link_webpage,title,doc_subtitle,doc_date,insert_date,doc_body,document_page,page_position,ticker,market)
    import csv

    with open("call_transcripts11", "w") as f:
        
         writer = csv.writer(f)
         for row in rows:
             writer.writerow(row)
               
               
#     with open('call_transcripts1') as csv_file: # load into mysql not succesefull   
#          
#        csv_reader = csv.reader(csv_file, delimiter=',')
#     for row in csv_reader:
#         mycursor.execute('use call_transcripts')
#         mycursor.execute('INSERT INTO nova12(link_webpage,title,doc_subtitle,doc_date,insert_date,doc_body,document_page,page_position,ticker,market) values ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")',row)
#     mydb.commit()
               
    import schedule #schedule function scrapper to run every day
    import time
    schedule.every().day.at("13:00").do(scraper) 
    while 1:
         
        
       schedule.run_pending()
       time.sleep(1)
  ################################################################################        making a daraframe to import into mysql
      
   rows = zip(link_webpage,title,doc_date,insert_date,ticker)
   import csv

   with open("call_transcripts11", "w") as f:
       
       
        
       writer = csv.writer(f)
       for row in rows:
           
           writer.writerow(row)
