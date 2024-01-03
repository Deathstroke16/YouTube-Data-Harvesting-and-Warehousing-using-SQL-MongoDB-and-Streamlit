############################################importing the necessary libraries###########################################
from datetime import timedelta
import logging
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build
from PIL import Image

########################################### SETTING PAGE CONFIGURATIONS ###########################################
icon = Image.open("youtube-logo.png")
st.set_page_config(
    page_title="YouTube Data Harvesting and Warehousing using Streamlit",
    page_icon=icon,
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={'About': """#Author Swapnil Aknurwar*"""})

############################################ CREATING OPTION MENU###########################################
with st.sidebar:
    selected = option_menu(None, ["Home", "Extract and Transform", "View"],
                           icons=["house-door-fill", "tools", "card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={
                               "nav-link": {"font-size": "18px", "text-align": "center", "margin": "0px", "--hover-color": "#C80101"},
                               "icon": {"font-size": "24px"},
                               "container": {"max-width": "6000px"},
                               "nav-link-selected": {"background-color": "#C80101"}})

############################################ Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)###########################################
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['youtube_data']

############################################ CONNECTING WITH MYSQL DATABASE###########################################
mydb = sql.connect(host="localhost",
                   user="root",
                   password="root",
                   database= "youtube_db"
                  )
mycursor = mydb.cursor(buffered=True)

############################################ BUILDING CONNECTION WITH YOUTUBE API###########################################
api_key = "AIzaSyAVLSg5UcgPCgUJJrI6IpueEoPz0iiGI-g"
youtube = build('youtube','v3',developerKey=api_key)


############################################ FUNCTION TO GET CHANNEL DETAILS###########################################
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id = channel_id[i],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data


############################################[ FUNCTION TO GET VIDEO IDS ]###########################################
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


############################################[ FUNCTION TO GET VIDEO DETAILS ]###########################################
def get_video_details(v_ids):
    video_stats = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats


############################################[ FUNCTION TO GET COMMENT DETAILS ]###########################################
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data


############################################[ FUNCTION TO GET CHANNEL NAMES FROM MONGODB ]###########################################
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name


if selected == "Home":
    st.image("yt1.gif", width=500)
    st.title("YouTube Data Harvesting and Warehousing")
    st.markdown("Retrieve YouTube data, store in MongoDB, transform to MySQL, and visualize with Streamlit.")
    st.markdown("## :blue_book: Domain: Social Media")
    st.markdown("## :toolbox: Technologies: Python, MongoDB, YouTube API, MySQL, Streamlit")
    
    
############################################[ EXTRACT and TRANSFORM PAGE ]###########################################
if selected == "Extract and Transform":
    tab1, tab2 = st.tabs(["$\huge📤 EXTRACTION$", "$\huge🔄 TRANSFORMATION$"])
    
    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID:")
        ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')

        if ch_id and st.button("Extract Details"):
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)

        if st.button("Upload data to MongoDB"):
            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)
                
                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d+= get_comments_details(i)
                    return com_d
                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                st.success("Upload to MogoDB successful !!")
      
    ############################################[ TRANSFORM TAB ]###########################################
    with tab2:     
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
        
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel",options= ch_names)
        
        def insert_into_channels():
                collections = db.channel_details
                query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
                
                for i in collections.find({"Channel_name" : user_inp},{'_id':0}):
                    mycursor.execute(query,tuple(i.values()))
                    mydb.commit()
        # Change default encoding of console to UTF-8
        import sys
        sys.stdout.reconfigure(encoding='utf-8')
        # Function to convert ISO 8601 duration to 'HH:MM:SS' format
        def convert_duration(iso_duration):
            delta = timedelta()
            duration = iso_duration[2:]  # Removing the 'PT' part

            hours, minutes, seconds = 0, 0, 0

            # Check if 'H', 'M', and 'S' components exist
            if 'H' in duration:
                hours_part, _, duration = duration.partition('H')
                hours = int(hours_part)
            if 'M' in duration:
                minutes_part, _, duration = duration.partition('M')
                minutes = int(minutes_part)
            if 'S' in duration:
                seconds_part, _, _ = duration.partition('S')
                seconds = int(seconds_part)

            delta += timedelta(hours=hours, minutes=minutes, seconds=seconds)

            return str(delta)

        ############################################[ Function to handle encoding issues ]###########################################
        def safe_encode(value):
            try:
                # Try to encode the value as UTF-8 and decode it back
                return value.encode('utf-8').decode('utf-8')
            except UnicodeEncodeError:
                # If encoding fails, replace non-printable characters with '?'
                return "".join(char if char.isprintable() else '?' for char in value)
        def insert_into_videos():
            collection = db.video_details            
            for video in collection.find({"Channel_name": user_inp}):
                video_id = video.get('Video_id', '')
                channel_name = video.get('Channel_name', '')
                channel_id = video.get('Channel_id', '')
                title = video.get('Title', '')
                tags = video.get('Tags', '')
                thumbnail = video.get('Thumbnail', '')
                description = video.get('Description', '')
                published_date = video.get('Published_date', '')
                duration = convert_duration(video.get('Duration', ''))
                views = video.get('Views', 0)
                likes = video.get('Likes', 0)
                comments = video.get('Comments', 0)
                favorite_count = video.get('Favorite_count', 0)
                definition = video.get('Definition', '')
                caption_status = video.get('Caption_status', '')

                # Use logging to print values for debugging
                logging.debug(f"Values: {video_id}, {channel_name}, {channel_id}, {title}, {tags}, {thumbnail}, {description}, {published_date}, {duration}, {views}, {likes}, {comments}, {favorite_count}, {definition}, {caption_status}")

                ############################################[ Convert the list of tags to a string ]###########################################
                tags_list = video.get('Tags', [])
                tags_str = ', '.join(tags_list) if tags_list is not None else ''
                try:
                    # Insert data into the MySQL 'videos' table
                    mycursor.execute("""
                        INSERT INTO videos (
                            channel_name, channel_id, video_id, title, tags, thumbnail, 
                            description, published_date, duration, views, likes, comments, 
                            favorite_count, definition, caption_status
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (channel_name, channel_id, video_id, title, tags_str, thumbnail, 
                        description, published_date, duration, views, likes, comments, 
                        favorite_count, definition, caption_status))

                    mydb.commit()

                    print("Data inserted successfully.")
                except Exception as e:
                    print(f"Error inserting video: {e}")

        def insert_into_comments():
            video_collection = db.video_details
            comments_collection = db.comments_details
            # Function to handle encoding issues
            def safe_encode(value):
                try:
                    # Try to encode the value as UTF-8 and decode it back
                    return value.encode('utf-8').decode('utf-8')
                except UnicodeEncodeError:
                    # If encoding fails, replace non-printable characters with '?'
                    return "".join(char if char.isprintable() else '?' for char in value)
            
            for video in video_collection.find({"Channel_name": user_inp}):
                video_id = video.get('Video_id', '')
                for comment in comments_collection.find({"Video_id": video_id}):
                    video_id = comment.get('Video_id', '')
                    comment_text = comment.get('Comment_text', '')
                    comment_author = comment.get('Comment_author', '')
                    comment_posted_date = comment.get('Comment_posted_date', '')
                    like_count = comment.get('Like_count', 0)
                    reply_count = comment.get('Reply_count', 0)

                    safe_comment_text = safe_encode(comment_text)

                    try:
                        # Insert data into the MySQL table with 'comment_id'
                        mycursor.execute("""
                                INSERT INTO comments (
                                    video_id, comment_text, comment_author,
                                    comment_posted_date, like_count, reply_count
                                ) VALUES (%s, %s, %s, %s, %s, %s)
                            """, (video_id, safe_comment_text, comment_author, comment_posted_date, like_count, reply_count))

                        mydb.commit()

                        print("Data inserted successfully.")
                    except Exception as e:
                        print(f"Error inserting comment: {e}")

        if st.button("Submit"):
            try:
                
                insert_into_channels()
                insert_into_videos()
                insert_into_comments()
                st.success("Transformation to MySQL Successful!!!")
            except:
                st.error("Channel details already transformed!!")
            
############################################[ VIEW PAGE ]###########################################
if selected == "View":
    # Additional Styling
    st.markdown(
    """
    <style>
        body {
            background-color: #f4f4f4;
        }
        .sidebar .sidebar-content {
            background-color: #1E1E1E;
            color: #FFFFFF;
        }
        .main .block-container {
            max-width: 1200px;
            margin: 0 auto;
        }
    </style>
    """,
    unsafe_allow_html=True
)
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name FROM videos ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name 
        AS Channel_Name, total_videos AS Total_Videos
                            FROM channels
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                            FROM videos
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.title AS Video_Title, b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                            FROM videos
                            ORDER BY likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title AS Title, likes AS Likes_Count
                            FROM videos
                            ORDER BY likes DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name
                            FROM videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT 
    channel_name AS Channel_Name,
    SEC_TO_TIME(SUM(TIME_TO_SEC(duration))) AS "Average_Video_Duration (hh:mm:ss)"
    FROM 
    videos
    GROUP BY 
    channel_name
    ORDER BY 
    AVG(duration) DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names                          )
        st.write(df)
        st.write("### :green[Average video duration for channels :]") 

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
