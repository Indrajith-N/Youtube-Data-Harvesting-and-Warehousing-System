from googleapiclient.discovery import build
import pandas as pd
import streamlit as st
import pymongo as pm
import psycopg2

## API CONNECTION

def Connect_API():    
    Api_id="Api Key"
    Api_serviceName="Youtube"
    Api_version="v3"
    connect=build(Api_serviceName,Api_version,developerKey=Api_id)
    return connect
youtube=Connect_API()

## CHANNEL INFORMATION

def get_channel_details(channel_id):
    request=youtube.channels().list(
                                part="brandingSettings,contentDetails,contentOwnerDetails,id,localizations,snippet,statistics,status,topicDetails",
                                id=channel_id
                                )
    response=request.execute()
    for i in response['items']:
        data={
            "Channel_Name": i['snippet']['title'],
            "Channel_Id": i["id"],
            "Subscription_Count": i['statistics']['subscriberCount'],
            "Channel_Views": i['statistics']['viewCount'],
            "Channel_Videos":i['statistics']['videoCount'],
            "Channel_Description": i['snippet']['description'],
            "Playlist_Id": i['contentDetails']['relatedPlaylists']['uploads']
        }
    return data

## VIDEO IDS

def get_videoIds(channel_id):    
    response=youtube.channels().list(
        part='contentDetails,snippet,statistics',
        id=channel_id
    )
    playlist_id=response.execute()
    playlist_ids=playlist_id['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    video_ids=[]
    next_page_token=None
    while True:
        response1=youtube.playlistItems().list( part="snippet",
            playlistId=playlist_ids,
            maxResults=50,                                              #maxResults is used to get 1-50 videos.
            pageToken=next_page_token).execute()
       
        
        for i in range (len(response1['items'])):
            r=response1['items'][i]['snippet']['resourceId']['videoId']
            video_ids.append(r)
        next_page_token=response1.get('nextPageToken')
        if next_page_token==None:
            break
    return(video_ids)


## VIDEO DETAILS

def get_videoDetails(videoIds):
    videos=[]   
    for video_id in videoIds:
        request1 = youtube.videos().list(
            part="contentDetails,snippet,statistics",
            id=video_id
        )
        r=request1.execute()
        for items in r['items']:
            data={
                "Channel_Name":items["snippet"]["channelTitle"],
                "Channel_Id":items['snippet']['channelId'],
            "Video_Id":items['id'],
            "Video_Name": items['snippet']['title'],
            "Video_Description":items['snippet']['description'],
            "Tags":items['snippet'].get('tags'),
            "PublishedAt": items['snippet']['publishedAt'],
            "View_Count":items['statistics']['viewCount'],
            "Like_Count": items['statistics'].get('likeCount'),
            "Dislike_Count": items.get("dislikesCount"),
            "Favorite_Count": items['statistics']['favoriteCount'],
            "Definition": items['contentDetails']['definition'],
            "Duration": items['contentDetails']['duration'],
            "Thumbnail":items['snippet']['thumbnails']['default']['url'],
            "Caption_Status": "Available"
            }
        videos.append(data)
    return videos


## COMMENTS DETAILS

def comments_info(videoIds):
    comments=[]
    for video_id in videoIds:
        try:
            request = youtube.commentThreads().list(
                part='snippet,replies',
                videoId=video_id,
                maxResults=100
            )
            response = request.execute()

            for item in response['items']:
                data = {
                    "Video_id":video_id,
                    "Comment_Id": item['snippet']['topLevelComment']['id'],
                    "Comment_Text": item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "Comment_Author": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_PublishedAt": item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                comments.append(data)

        except:
            pass
    return comments

## PLAYLIST DETAILS

def PlaylistDetails(channel_id):
    next_page_token=None
    playlist_details=[]
    while True:    
        request=youtube.playlists().list(
                        part="snippet,contentDetails",
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token)
        response=request.execute()
        for i in  response['items']:
            data={"Channel_Id":i['snippet']['channelId'],
                  "Channel_Name":i['snippet']['channelTitle'],
                "Playlist_Id":i['id'],
                "Published_At":i['snippet']['publishedAt'],
                "Video_Count":i['contentDetails']['itemCount']
                }
            playlist_details.append(data)
        next_page_token=response.get('nextPageToken')    
        if next_page_token==None:
            break
    return playlist_details


## CONNECTION TO MONGO DB

client=pm.MongoClient("mongodb://localhost:27017")
db=client["YoutubeData"]

## TRANSFER DATA TO MONGO DB

def ImportChannelDetails(channel_id):
    ch_details=get_channel_details(channel_id)
    vid_ids=get_videoIds(channel_id)
    playli_details=PlaylistDetails(channel_id)
    vid_details=get_videoDetails(vid_ids)
    com_details=comments_info(vid_ids)
    
    cl=db['channel_details']
    cl.insert_one({"Channel_Information":ch_details,
                  "Video_Information":vid_details,
                  "Comments_Information":com_details,
                  "Playlist_Information":playli_details})
    return "Uploaded Successfully"

## CONVERT CHANNEL INFORMATION TO SQL

channel_data=[]
cl=db['channel_details']
for ch_data in cl.find({},{"_id":0,"Channel_Information":1}):
    channel_data.append(ch_data['Channel_Information'])

df1=pd.DataFrame(channel_data)


def YoutubeChannelDetails():
                                              #Connection with pg andmin SQL
  mydb=psycopg2.connect( host="localhost",
                      database="jithu007",
                        user="postgres",
                        port="5432",
                        password="Indran48@")
  mycursor=mydb.cursor()
  
                                              #Drop table to add more Details
  drop_table='''drop table if exists channels'''
  mycursor.execute(drop_table)
  mydb.commit()
                                              #Create new Table with Same name 
  try:    
    mytable='''Create table if not exists Channels(Channel_Name varchar(50),
                                                    Channel_Id varchar(50) primary key,
                                                    Subscription_Count bigint,
                                                    Channel_Views bigint,
                                                    Channel_Videos bigint,
                                                    Channel_Description text,
                                                    Playlist_Id varchar(50)
                                                )'''
    mycursor.execute(mytable)
    mydb.commit()
  except:
      print("table already created")

                                                #Insert rows into the table
  for index,row in df1.iterrows():
      query='''insert into channels(                  Channel_Name ,
                                                      Channel_Id ,
                                                      Subscription_Count,
                                                      Channel_Views ,
                                                      Channel_Videos ,
                                                      Channel_Description,
                                                      Playlist_Id )
                                          values(%s,%s,%s,%s,%s,%s,%s)'''
      values=(row['Channel_Name'],
              row['Channel_Id'],
              row['Subscription_Count'],
              row['Channel_Views'],
              row['Channel_Videos'],
              row['Channel_Description'],
              row['Playlist_Id'])
      try:
          mycursor.execute(query, values)
          mydb.commit()  # Commit only if successful
      except Exception as e:
          print(f"Error: {e}")
          mydb.rollback()
  return "Uploaded  channel Successfully"

## CONVERT PLAYLISTS INFORMATION TO SQL

playlist_data=[]
cl=db['channel_details']
for pl_data in cl.find({},{"_id":0,"Playlist_Information":1}):
    for j in range(len(pl_data['Playlist_Information'])):
        playlist_data.append(pl_data['Playlist_Information'][j])
    
df2=pd.DataFrame(playlist_data)

def YoutubePlaylistDetails():
                                        #Connection with pg andmin SQL
    mydb=psycopg2.connect( host="localhost",
                        database="jithu007",
                        user="postgres",
                        port="5432",
                        password="Indran48@")
    mycursor=mydb.cursor()

                                                #Drop table to add more Details
    drop_table='''drop table if exists Playlists'''
    mycursor.execute(drop_table)
    mydb.commit()
                                                #Create new Table with Same name 
    try:    
        mytable='''Create table if not exists Playlists(Channel_Id varchar(50),
                                                        Channel_Name varchar(50) ,
                                                        Playlist_Id varchar(50) primary key,
                                                        Published_At timestamp,
                                                        Video_Count bigint
                                                    )'''
        
        mycursor.execute(mytable)
        mydb.commit()
    except:
        print("table already created")


                                                #Insert rows into the table

    for index,row in df2.iterrows():
        query='''insert into Playlists( Channel_Id ,
                                        Channel_Name  ,
                                        Playlist_Id ,
                                        Published_At ,
                                        Video_Count  )

                                        values(%s,%s,%s,%s,%s)'''
        values=(row['Channel_Id'],
                row['Channel_Name'],
                row['Playlist_Id'],
                row['Published_At'],
                row['Video_Count'])
        try:
            mycursor.execute(query, values)
            mydb.commit()  # Commit only if successful
        except Exception as e:
            print(f"Error: {e}")
            mydb.rollback()
    return "Uploaded  playlist Successfully"

## CONVERT VIDEO DETAILS TO SQL

video_data=[]
cl=db['channel_details']
for vi_data in cl.find({},{"_id":0,"Video_Information":1}):
    for j in range(len(vi_data['Video_Information'])):
        video_data.append(vi_data['Video_Information'][j])
    
df3=pd.DataFrame(video_data)


def YoutubeVideosDetails():                                           #Connection with pg andmin SQL
    mydb=psycopg2.connect( host="localhost",
                        database="jithu007",
                        user="postgres",
                        port="5432",
                        password="Indran48@")
    mycursor=mydb.cursor()

                                                #Drop table to add more Details
    drop_table='''drop table if exists Videos'''
    mycursor.execute(drop_table)
    mydb.commit()
                                                #Create new Table with Same name 
    try:    
        mytable='''Create table if not exists Videos(Channel_Name varchar(50),
                    Channel_Id varchar(50),
                Video_Id varchar(50) primary key ,
                Video_Name varchar(100),
                Video_Description text,
                Tags text,
                PublishedAt timestamp,
                View_Count bigint,
                Like_Count bigint,
                Dislike_Count bigint ,
                Favorite_Count bigint,
                Definition varchar(100),
                Duration interval,
                Thumbnail  varchar(100),
                Caption_Status varchar(100)
                                                    )'''
        
        mycursor.execute(mytable)
        mydb.commit()
    except:
        print("table already created")


                                                #Insert rows into the table

    for index,row in df3.iterrows():
        query='''insert into Videos(Channel_Name ,
                    Channel_Id ,
                Video_Id ,
                Video_Name ,
                Video_Description ,
                Tags ,
                PublishedAt ,
                View_Count ,
                Like_Count ,
                Dislike_Count  ,
                Favorite_Count ,
                Definition ,
                Duration ,
                Thumbnail  ,
                Caption_Status  )

                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Video_Name'],
                row['Video_Description'],
                row['Tags'],row['PublishedAt'],row['View_Count'],row['Like_Count'],row['Dislike_Count'],row['Favorite_Count'],row['Definition'],
                row['Duration'],row['Thumbnail'],row['Caption_Status'],)
        try:
            mycursor.execute(query, values)
            mydb.commit()  # Commit only if successful
        except Exception as e:
            print(f"Error: {e}")
            mydb.rollback()
    return "Uploaded  videos Successfully"

## CONVERT COMMENTS DETAILS TO SQL

cmt_data=[]
cl=db['channel_details']
for cm_data in cl.find({},{"_id":0,"Comments_Information":1}):
    for j in range(len(cm_data['Comments_Information'])):
        cmt_data.append(cm_data['Comments_Information'][j])
    
df4=pd.DataFrame(cmt_data)

def YoutubeCommentsDetails():                                         #Connection with pg andmin SQL
    mydb=psycopg2.connect( host="localhost",
                        database="jithu007",
                        user="postgres",
                        port="5432",
                        password="Indran48@")
    mycursor=mydb.cursor()

                                                #Drop table to add more Details
    drop_table='''drop table if exists Comments'''
    mycursor.execute(drop_table)
    mydb.commit()
                                                #Create new Table with Same name 
    try:    
        mytable='''Create table if not exists Comments( Video_id varchar(50) ,
                    Comment_Id varchar(100) primary key ,
                Comment_Text text ,
                Comment_Author varchar(100),
                Comment_PublishedAt timestamp
                                                    )'''
        
        mycursor.execute(mytable)
        mydb.commit()
    except:
        print("table already created")


                                                #Insert rows into the table

    for index,row in df4.iterrows():
        query='''insert into Comments(  Video_id ,
                    Comment_Id ,
                Comment_Text  ,
                Comment_Author ,
                Comment_PublishedAt  )

                                        values(%s,%s,%s,%s,%s)'''
        values=(row['Video_id'],
                row['Comment_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_PublishedAt'])
        try:
            mycursor.execute(query, values)
            mydb.commit()  # Commit only if successful
        except Exception as e:
            print(f"Error: {e}")
            mydb.rollback()
    return "Uploaded  comments Successfully"

## CALLING ALL ABOVE SQL DATA TRANSFER FUNCTIONS

def tables_SQL():
    YoutubeChannelDetails()
    YoutubePlaylistDetails()
    YoutubeVideosDetails()
    YoutubeCommentsDetails()
    return " tables Created"


##  SHOWING CHANNEL DETAILS IN STREAMLIT

def show_channel_details():
   
    channel_data=[]
    cl=db['channel_details']
    for ch_data in cl.find({},{"_id":0,"Channel_Information":1}):
        channel_data.append(ch_data['Channel_Information'])

    df1=st.dataframe(channel_data)
    return channel_data

##  SHOWING PLAYLIST DETAILS IN STREAMLIT

def show_playlists_details():
    playlist_data=[]
    cl=db['channel_details']
    for pl_data in cl.find({},{"_id":0,"Playlist_Information":1}):
        for j in range(len(pl_data['Playlist_Information'])):
            playlist_data.append(pl_data['Playlist_Information'][j])
        
    df2=st.dataframe(playlist_data)
    
    return playlist_data

##  SHOWING VIDEOS DETAILS IN STREAMLIT

def show_videos_details():

    video_data=[]
    cl=db['channel_details']
    for vi_data in cl.find({},{"_id":0,"Video_Information":1}):
        for j in range(len(vi_data['Video_Information'])):
            video_data.append(vi_data['Video_Information'][j])
        
    df3=st.dataframe(video_data)
    
    return video_data

##  SHOWING COMMENTS DETAILS IN STREAMLIT

def show_comments_details():

    cmt_data=[]
    cl=db['channel_details']
    for cm_data in cl.find({},{"_id":0,"Comments_Information":1}):
        for j in range(len(cm_data['Comments_Information'])):
            cmt_data.append(cm_data['Comments_Information'][j])
        
    df4=st.dataframe(cmt_data)
    return cmt_data


## STREAMLIT PART CODINGS


with st.sidebar:
    st.title(":blue[Youtube Harvesting and Warehouse]")
    st.header("Skill Enhancement")
    st.caption('Python Scripting')
    st.caption('Data Collection')
    st.caption('MongoDB')
    st.caption('API Integration')
    st.caption('Data Management using MongoDB & SQL')

channel_id=st.text_input("Enter the Channel ID")
if st.button("Collect & Store Data"):
    chl_ID=[]
    db=client["YoutubeData"]
    cl=db['channel_details']
    for ch_data in cl.find({},{"_id":0,"Channel_Information":1}):
        chl_ID.append(ch_data['Channel_Information']['Channel_Id'])
    if channel_id in chl_ID:
        st.success("Channel_id Already Exists")
    elif channel_id=='':
        st.success("Please enter channel ID")
    else:
        Insert=ImportChannelDetails(channel_id)
        st.success("Data Inserted")
if st.button("Transfer to SQL"):
    Tables=tables_SQL()
    st.success(Tables)

show_table=st.radio("SELECT THE TABLE",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_details()
if show_table=="PLAYLISTS":
    show_playlists_details()
if show_table=="VIDEOS":
    show_videos_details()
if show_table=="COMMENTS":
    show_comments_details()



mydb=psycopg2.connect( host="localhost",        
                    database="jithu007",
                    user="postgres",
                    port="5432",
                    password="Indran48@")     #Connection with pg andmin SQL
mycursor=mydb.cursor()


## 10 QUESTIONS IN SELECTION BOX

Questions=st.selectbox(("Select Your Question"),("1.What are the names of all the videos and their corresponding channels?",
                    "2.Which channels have the most number of videos, and how many videos do  they have?",
                    "3.What are the top 10 most viewed videos and their respective channels?",
                    "4.How many comments were made on each video, and what are their  corresponding video names?",
                    "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                    "6.What is the total number of likes for each video, and what are their corresponding video names?",
                    "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                    "8.What are the names of all the channels that have published videos in the year  2022?",
                    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
))

if Questions=="1.What are the names of all the videos and their corresponding channels?":
    query1="select channel_name as Name_of_the_Channel,video_name as Name_of_the_Video from videos  order by Name_of_the_Channel asc "
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    dfa=pd.DataFrame(t1,columns=["Channel_name","Name_of_Videos"])
    st.write(dfa)    

elif Questions=="2.Which channels have the most number of videos, and how many videos do  they have?":
    query1="select channel_name as Name_of_the_Channel,channel_videos as No_of_the_Video from channels  order by No_of_the_Video desc "
    mycursor.execute(query1)
    mydb.commit()
    t2=mycursor.fetchall()
    dfb=pd.DataFrame(t2,columns=["Channel_name","No_of_Videos"])
    st.write(dfb)

elif Questions=="3.What are the top 10 most viewed videos and their respective channels?":
    query1="select channel_name as Name_of_the_Channel,view_count as No_of_the_Views from videos order by No_of_the_Views desc limit 10 "
    mycursor.execute(query1)
    mydb.commit()
    t3=mycursor.fetchall()
    dfc=pd.DataFrame(t3,columns=["Channel_name","No_of_Videos"])
    st.write(dfc)

elif Questions=="4.How many comments were made on each video, and what are their  corresponding video names?":
    query1="select b.video_name,count(a.comment_text) from comments a inner join videos b on a.video_id=b.video_id group by b.video_name "
    mycursor.execute(query1)
    mydb.commit()
    t4=mycursor.fetchall()
    dfd=pd.DataFrame(t4,columns=["Video_Name","No_of_Comments"])
    st.write(dfd)
    
elif Questions=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query1="select channel_name,video_name,like_count from videos where like_count is not null order by like_count desc limit 1 "
    mycursor.execute(query1)
    mydb.commit()
    t5=mycursor.fetchall()
    dfe=pd.DataFrame(t5,columns=["Channel_name","Name_of_Videos","Like_count"])
    st.write(dfe)

elif Questions=="6.What is the total number of likes for each video, and what are their corresponding video names?":
    query1="select video_name,like_count from videos order by like_count desc "
    mycursor.execute(query1)
    mydb.commit()
    t6=mycursor.fetchall()
    dff=pd.DataFrame(t6,columns=["Video_name","No_of_Likes"])
    st.write(dff)

elif Questions=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query1="select channel_name,channel_views from channels  order by channel_views desc "
    mycursor.execute(query1)
    mydb.commit()
    t7=mycursor.fetchall()
    dfg=pd.DataFrame(t7,columns=["Channel_name","No_of_views"])
    st.write(dfg)
elif Questions=="8.What are the names of all the channels that have published videos in the year  2022?":
    query1="select channel_name,video_name,publishedat from videos where publishedat between '2022-01-01' and '2022-12-31'"
    mycursor.execute(query1)
    mydb.commit()
    t8=mycursor.fetchall()
    dfh=pd.DataFrame(t8,columns=["Channel_name","Video_name","published_at_2022"])
    st.write(dfh)
elif Questions== "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query1="select channel_name,avg(duration) from videos group by channel_name"
    mycursor.execute(query1)
    mydb.commit()
    t9=mycursor.fetchall()
    dfi=pd.DataFrame(t9,columns=["Channel_name","Avg_duration"])
    st.write(dfi)

    
    
    



