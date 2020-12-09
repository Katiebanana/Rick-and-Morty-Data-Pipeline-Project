#!/usr/bin/env python
# coding: utf-8

import requests
import json
import bs4
from bs4 import BeautifulSoup
import paralleldots
import sqlite3
import time
import csv
import argparse
import numpy as np
import pandas as pd

def main():
    # options to choose invoke --source local, --source remote or --grade
    parser = argparse.ArgumentParser(allow_abbrev=False)
    # setting --source to not be required because grader can just use the --grade option without running the entire program
    parser.add_argument('--source', type=str, choices=["local","remote"],  required=False, help="choose between local or remote")
    parser.add_argument('--grade',required=False,action='store_true',help='Revoking this flag will only process 3 hits for each scrape/API')
    args = parser.parse_args()
    option = args.source
    
    #get only 3 rows for each datasets
    if args.grade==True: 
        def paralleldots_setup():
            paralleldots.set_api_key( "nDaUF9bcLcrvQevZcqOBMxLTzIPAnckVsBXeMxvAv6c" )
        paralleldots_setup()
        
        
        # Cannabis API scraping
        def weed_API():
            weedapi='QPdL0dH'
            urlweed=f'http://strainapi.evanbusse.com/{weedapi}/strains/search/all'
            try: #try if the url is workable
                resp = requests.get(urlweed)
                resp.raise_for_status()
            except requests.exceptions.HTTPError as err:
                return err
            strainresult=resp.json() 
            return strainresult
        
        # collects strain IDs and names into tuple pairs for sql table
        def get_strain_id():
            strain_id_sql=[]
            for key,value in weed_API().items():
                strain_id_sql.append(weed_API()[key]['id'],key)
            return strain_id_sql()
        
        
        # gets URL for each episode -- episode number as parameter
        def episodeurl(num):
            episodeapi=f"https://rickandmortyapi.com/api/episode/{num}"
            try: #try if the url is workable
                resp=requests.get(episodeapi)
                resp.raise_for_status()
            except requests.exceptions.HTTPError as err: 
                return err
            episode=resp.json()
            name=episode['name']
            if num!=19 and num!=25 and num!=32 and num!=35: # These are the episodes where the name in api and plot website are different
                name=name.replace(":","")
            name=name.replace(",","")
            name=name.split()
            episode_name="_".join(name)
            episodeurl=f'https://rickandmorty.fandom.com/wiki/{episode_name}'
            return episodeurl
        
        # finding sentiment in plot summary -- episode number as parameter
        def plotemo(num):
            try:
                content = requests.get(episodeurl(num))
                content.raise_for_status()
            except requests.exceptions.HTTPError as err: 
                return err
            soup = BeautifulSoup(content.content, 'html.parser')
            summary=soup.find_all('p')[3:]
            emotiondic=paralleldots.emotion(summary)
            return emotiondic
        
        # creates connection to sql database
        def create_connection():
            conn=sqlite3.connect('510FinalProject.db')
            cur=conn.cursor()
            return (conn,cur)
        
        # Uses Paralleldots to get emotion for each episode throught text analysis of plot summary
        def episode_emotion_grade(): 
            conn, cur=create_connection()
            try:
                cur.execute("DROP TABLE IF EXISTS episode_emotion_table")
                cur.execute("CREATE TABLE episode_emotion_table (episode_id INTEGER, happy_id INTEGER, happy real,angry_id INTEGER, angry real,bored_id INTEGER, bored real,fear_id INTEGER, fear real,sad_id INTEGER, sad real, excited_id INTEGER,excited real)")
            except requests.exceptions.HTTPError as err: 
                return err
            num=1
            while num<=3: # input only 3 rows of data
                plotdic=plotemo(num)
                if "emotion" not in plotdic: #API hit hits limit. Quite program and prompt user to try again later
                    print("Paralleldots API is out of hits, please wait one minute and try again")
                    return
                cur.execute("INSERT INTO episode_emotion_table VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",(num,1,plotdic['emotion']['Happy'],2,plotdic['emotion']['Angry'],3,plotdic['emotion']['Bored'],4,plotdic['emotion']['Fear'],5,plotdic['emotion']['Sad'],6,plotdic['emotion']['Excited']))     
                num+=1
            cur.execute("SELECT * FROM episode_emotion_table")
            print(f"Data in episode_emption_table: {cur.fetchall()}")
            conn.commit()
            conn.close()
           
        episode_emotion_grade()
        
        def leaflyurl(strainname): 
            leaflyurl=f'https://www.leafly.com/strains/{strainname}/reviews'
            return leaflyurl
        
        # function to find emotion for each strain review
        def review_emotion(strainname): 
            reviewdic={}
            cnt=1
            content=requests.get(leaflyurl(strainname))
            soup = BeautifulSoup(content.content, 'html.parser')
            if soup.find_all(itemprop="reviewBody")==[]: # this means that the url is faulty or the leafly does not contain review for this strain
                return

            for review in soup.find_all(itemprop="reviewBody"):
                emotions=paralleldots.emotion(review.text)
                if 'emotion' in reviewdic: # this is for the second and third reviews, when the key 'emotion' already exists
                    for emo in emotions['emotion']:
                        reviewdic['emotion'][emo]+=emotions['emotion'][emo]
                else: # this is for the first review, initializes the dictionary
                    reviewdic.update(emotions)
                cnt+=1
                if cnt>2:  # this limits the program to only scrape 2 reviews to save API hit limit
                    break
            for emo in reviewdic['emotion']: #calculates average emotion scores from total number of reviews
                reviewdic['emotion'][emo]=reviewdic['emotion'][emo]/len(soup.find_all(itemprop="reviewBody"))
            return reviewdic
        
        
        def available_strain_grade():
            available_strain2=[]
            for strain in weed_API(): #only take 3 strains
                strainname=strain.lower()
                content=requests.get(leaflyurl(strainname))
                soup = BeautifulSoup(content.content, 'html.parser')
                if soup.find_all(itemprop="reviewBody")!=[]: # this means that the url is faulty or the leafly does not contain review for this strain
                    available_strain2.append(strain)
                if len(available_strain2)==3:
                    break
            return available_strain2 
        available_strain=available_strain_grade()
        print(f"available strains are: {available_strain}")
        
        # takes 3 rows of data for strain review emotions
        def strain_review_emotion_grade(): 
            conn, cur=create_connection()
            try:
                cur.execute("DROP TABLE IF EXISTS strain_review_table")
                cur.execute("CREATE TABLE strain_review_table (strain_name TEXT, happy_id INTEGER, happy real,angry_id INTEGER, angry real,bored_id INTEGER, bored real,fear_id INTEGER, fear real,sad_id INTEGER, sad real, excited_id INTEGER,excited real)")
            except requests.exceptions.HTTPError as err: 
                return err
            for strain in available_strain:
                strain2=strain.lower() # this create a strain name compatible for scraping in Leafly
                #code to check if strain2 is in strain_name column
                query = f"SELECT * FROM strain_review_table WHERE strain_name = \'{strain2}\'"
                cur.execute(query)
                if len(cur.fetchall())==0: #if the row does not exist, insert row
                    try:
                        plotdic=review_emotion(strain2)
                        if "emotion" not in plotdic: #API hit hits limit. Quite program and prompt user to try again later
                            print("Paralleldots API is out of hits, please wait one minute and try again")
                            return
                        cur.execute("INSERT INTO strain_review_table VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",(strain2,1,plotdic['emotion']['Happy'],2,plotdic['emotion']['Angry'],3,plotdic['emotion']['Bored'],4,plotdic['emotion']['Fear'],5,plotdic['emotion']['Sad'],6,plotdic['emotion']['Excited']))
                        print("Whooo executed")
                    except (KeyError, TypeError):
                        continue
                else: 
                    continue
            print("Available data has been added to 510FinalProject.db")
            cur.execute("SELECT * FROM strain_review_table")
            print(f"Data in strain_review_table: {cur.fetchall()}")
            conn.commit()
            conn.close()
        strain_review_emotion_grade()
        
        

    elif option == "remote":
        start_time = time.time()

        # paralleldots API key initialization
        def paralleldots_setup():
            paralleldots.set_api_key( "nDaUF9bcLcrvQevZcqOBMxLTzIPAnckVsBXeMxvAv6c" )
            print("paralleldots is set up")
        paralleldots_setup()
        
        # Cannabis API scraping
        def weed_API():
            weedapi='QPdL0dH'
            urlweed=f'http://strainapi.evanbusse.com/{weedapi}/strains/search/all'
            try: #try if the url is workable
                resp = requests.get(urlweed)
                resp.raise_for_status()
            except requests.exceptions.HTTPError as err:
                return err
            strainresult=resp.json()
            print("successfully set up Marajuana Strain API")
            return strainresult
        
        # collects strain IDs and names into tuple pairs for sql table
        def get_strain_id():
            strain_id_sql=[]
            for key,value in weed_API().items():
                strain_id_sql.append((weed_API()[key]['id'],key))
            return strain_id_sql()
        
        
        # gets URL for each episode -- episode number as parameter
        def episodeurl(num):
            episodeapi=f"https://rickandmortyapi.com/api/episode/{num}"
            try: #try if the url is workable
                resp=requests.get(episodeapi)
                resp.raise_for_status()
            except requests.exceptions.HTTPError as err: 
                return err
            episode=resp.json()
            name=episode['name']
            if num!=19 and num!=25 and num!=32 and num!=35: # These are the episodes where the name in api and plot website are different
                name=name.replace(":","")
            name=name.replace(",","")
            name=name.split()
            episode_name="_".join(name)
            episodeurl=f'https://rickandmorty.fandom.com/wiki/{episode_name}'
            return episodeurl
        
        # finding sentiment in plot summary -- episode number as parameter
        def plotemo(num):
            try:
                content = requests.get(episodeurl(num))
                content.raise_for_status()
            except requests.exceptions.HTTPError as err: 
                return err
            soup = BeautifulSoup(content.content, 'html.parser')
            summary=soup.find_all('p')[3:]
            return paralleldots.emotion(summary)
        
        # creates connection to sql database
        def create_connection():
            print("connected successfully to SQL database")
            conn=sqlite3.connect('510FinalProject.db')
            cur=conn.cursor()
            return (conn,cur)
  
        
        #sets up emotion ids for 6 emotions
        def emotion_id():
            print("creating emotion_id_table")
            conn, cur=create_connection()
            cnt=1
            try:
                cur.execute("DROP TABLE IF EXISTS emotion_table")
                cur.execute("CREATE TABLE emotion_table (emotion_id INTEGER PRIMARY KEY, emotion_name TEXT)")
            except sqlite3.OperationalError as err:
                return err
            hello=paralleldots.emotion("Hello")
            if "emotion" not in hello: #API hit hits limit. Quite program and prompt user to try again later
                print("Paralleldots API is out of hits, please wait one minute and try again")
                return
            for key in hello['emotion']:
                cur.execute('INSERT INTO emotion_table VALUES(?,?)',(cnt,key))
                cnt+=1
            conn.commit()
            conn.close()
            print("emotion_table is added to 510FinalProject.db")
        emotion_id()
            
        # sets up episode IDs
        def episode_id():
            print("creating episode_id_table")
            conn, cur=create_connection()
            try: 
                cur.execute("DROP TABLE IF EXISTS episode_table")
                cur.execute("CREATE TABLE episode_table (episode_id INTEGER PRIMARY KEY, episode_name TEXT)")
            except sqlite3.OperationalError as err:
                return err
            num=1
            while num<=41:
                episodeapi=f"https://rickandmortyapi.com/api/episode/{num}"
                try:
                    resp=requests.get(episodeapi)
                    resp.raise_for_status()
                except requests.exceptions.HTTPError as err: 
                    return err
                episode=resp.json()
                name=episode['name']
                if num!=19 and num!=25 and num!=32 and num!=35: # These are the episodes where the name in api and plot website are different
                    name=name.replace(":","")
                name=name.replace(",","")
             
                cur.execute("INSERT INTO episode_table VALUES(?,?)",(num,name))
                num+=1
            conn.commit()
            conn.close()
            print("episode_table is added to 510FinalProject.db")
        episode_id()
        
      
        # Uses Paralleldots to get emotion for each episode throught text analysis of plot summary
        def episode_emotion(): #takes a while to input all data
            print("creating episode_emotion_table")
            conn, cur=create_connection()
            try:
                cur.execute("DROP TABLE IF EXISTS episode_emotion_table")
                cur.execute("CREATE TABLE episode_emotion_table (episode_id INTEGER, happy_name TEXT, happy real,angry_name TEXT, angry real,bored_name TEXT, bored real,fear_name TEXT, fear real,sad_name TEXT, sad real, excited_name TEXT,excited real)")
            except sqlite3.OperationalError as err:
                return err
            num=1
            while num<=41: # there are 41 episodes
                plotdic=plotemo(num)
                if "emotion" not in plotdic: #API hit hits limit. Quite program and prompt user to try again later
                    print("Paralleldots API is out of hits, please wait one minute and try again")
                    return
                try:
                    cur.execute("INSERT INTO episode_emotion_table VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",(num,"happy",plotdic['emotion']['Happy'],"angry",plotdic['emotion']['Angry'],"bored",plotdic['emotion']['Bored'],"fear",plotdic['emotion']['Fear'],"sad",plotdic['emotion']['Sad'],"excited",plotdic['emotion']['Excited'])) 
                except OperationalError as err:
                    return err
                num+=1
            conn.commit()
            conn.close()
            print("episode_emotion_table is added to 510FinalProject.db")
        episode_emotion()
            
        # gets strain review URL (strain name needs to be all lower case)
        def leaflyurl(strainname): 
            leaflyurl=f'https://www.wikileaf.com/strain/{strainname}'
            return leaflyurl
        
        # function to find emotion for each strain review
        def review_emotion(strainname): 
            content=requests.get(leaflyurl(strainname))
            soup = BeautifulSoup(content.content,'html.parser')
            if soup.find_all("p")[4:9]==[]: # this means that the url is faulty or the leafly does not contain review for this strain
                print("Wiki Leaf url is faulty") 
                return
            summary=soup.find_all("p")[4:9]
            return paralleldots.emotion(summary)
    
        
        
        # Function to get all the strains that have reviews on Leafly (takes 13 minutes)
        def available_strain():
            print("Going to take about 7 minutes to get your results")
            start_time = time.time()
            available_strain2=[]
            available_strain_dict={}
            for strain in weed_API(): #(13 minutes)gets all the strain names that have reviews on Leafly, takes a while since there are 1970+ strains
                if len(strain.split())>1: #if the strain name has more than two words, the naming convention on website changes for each strain, so we skip it to protect our program
                    continue
                strainname=strain.lower()
                content=requests.get(leaflyurl(strainname))
                soup = BeautifulSoup(content.content, 'html.parser')
                if soup.find_all("p")[4:9]!=[]: # this means that the url is faulty or the leafly does not contain review for this strain
                    available_strain2.append(strain)
                    print(strain, "added to available strain")
                if len(available_strain2)==300: #we want 300 strains max to save time
                    return available_strain2
            elapsedtime=time.time()-start_time
            print(f"It took me {elapsedtime} seconds to compile {len(available_strain2)} available strains")
            # save available strain list to panda dataframe
            df_episode = pd.DataFrame(available_strain2)
            #following code saves it to csv, which we can then use for future code without having to run this over and over again
            df_episode.to_csv("available_strain.csv",index=False)
            print("all strains saved to available_strain.csv")
            return 
        available_strain()
        
        # DO NOT RUN THIS MORE THAN ONCE
        def create_strain_review_table(): #takes a while to input all data
            print("creating strain_review_table")
            conn, cur=create_connection()
            try:
                cur.execute("DROP TABLE IF EXISTS strain_review_table")
                cur.execute("CREATE TABLE strain_review_table (strain_id INT, strain_name TEXT, happy_name TEXT, happy real,angry_name TEXT, angry real,bored_name TEXT, bored real,fear_name TEXT, fear real,sad_name TEXT, sad real, excited_name TEXT,excited real)")
            except sqlite3.OperationalError as err:
                return err
            conn.commit()
            conn.close()
        create_strain_review_table()
            
        def strain_review_emotion(): #takes a while to input all data
        
            print("inputing data for strain_review_table")
            conn, cur=create_connection()
            # try to access the file we created in function available strain
            try:
                available_strain=open("available_strain.csv","r")
            except:
                return "file is not available"
            #first line of csv file is header, we do not include
            available_strain.readline()
   
            for strain in available_strain:
                strain2=strain.lower().strip() # this create a strain name compatible for scraping in Leafly
                # try for weed_API() errors
                try:
                    strain_id=weed_API()[strain2.capitalize()]["id"]
                    print(f"we got id {strain_id}")
                except (KeyError, TypeError):
                    continue
                query = f"SELECT * FROM strain_review_table WHERE strain_name = \'{strain2}\'"
                cur.execute(query)
                #code to check if strain2 is in strain_name column
                if len(cur.fetchall())==0: #if the row does not exist, insert row
                    try:
                        print(strain2)
                        plotdic=review_emotion(strain2)
                        if "emotion" not in plotdic: #API hit hits limit. Quite program and prompt user to try again later
                            print("Paralleldots API is out of hits, please wait one minute and try again")
                            return 
                        print(plotdic)
                        try:
                            cur.execute("INSERT INTO strain_review_table VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(strain_id,strain2,"happy",plotdic['emotion']['Happy'],"angry",plotdic['emotion']['Angry'],"bored",plotdic['emotion']['Bored'],"fear",plotdic['emotion']['Fear'],"sad",plotdic['emotion']['Sad'],"excited",plotdic['emotion']['Excited']))
                        except OperationalError as err:
                            return err
                        print("Whooo executed")
                    except (KeyError, TypeError):
                        continue
                else: 
                    continue

            print("strain_review_table is added to 510FinalProject.db")
            conn.commit()
            conn.close()
  
        strain_review_emotion()
    
        # tells user the total time it took to run the program
        elapsedtime=time.time()-start_time
        print(f"It took me {elapsedtime} seconds to run all website and API scraping")

    elif option == "local":
        def create_connection():
            conn=sqlite3.connect('510FinalProject.db')
            cur=conn.cursor()
            return (conn,cur)
        def fetch_tables():
            conn, cur=create_connection()
            cur.execute('SELECT name from sqlite_master where type= "table"')
            print(f"These are the tables ready for use in 510FinalProject.db: {cur.fetchall()}")
        fetch_tables()
        create_connection()
          
if __name__ == "__main__":
    main()
