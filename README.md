# YouTube Data Harvesting and Warehousing using SQL, MongoDB, and Streamlit

## Problem Statement

The goal of this project is to develop a Streamlit application that allows users to analyze data from multiple YouTube channels. Users should be able to input a YouTube channel ID to access information such as channel details, video details, and user engagement metrics. The application should support storing the data in a MongoDB database and provide the functionality to collect data from up to 10 different channels. Additionally, it should allow users to migrate selected channel data from the data lake to a SQL database for further analysis. The application should enable searching and retrieval of data from the SQL database, including advanced options like joining tables for comprehensive channel information.

## Technology Stack

1. **Python**: Used for developing the Streamlit application and implementing backend logic.
2. **MySQL**: Utilized as the SQL database for storing structured data.
3. **MongoDB**: Chosen for handling unstructured and semi-structured data in the form of a data lake.
4. **Google Client Library**: Employed for establishing a connection to the YouTube API V3 and retrieving channel and video data.

## Approach

1. **Streamlit Application Setup**: Initialize a Streamlit application that allows users to input a YouTube channel ID, view channel details, and select channels for data migration.

2. **YouTube API Connection**: Establish a connection to the YouTube API V3 using the Google API client library for Python. Retrieve channel and video data based on the provided channel ID.

3. **MongoDB Data Lake**: Store the retrieved data in a MongoDB data lake. Create three collections in MongoDB: one for channel details, one for video details, and one for comments details.

4. **SQL Data Warehouse**: Transfer the collected data from multiple channels (channels, videos, and comments) to a SQL data warehouse. Utilize a SQL database (e.g., MySQL) for this purpose. Properly structure the SQL tables with primary and foreign keys.

5. **SQL Queries for Analysis**: Use SQL queries to join tables within the SQL data warehouse and retrieve specific channel data based on user input. Leverage the capabilities of SQL to perform comprehensive data analysis.

6. **Streamlit Data Visualization**: Display the retrieved data within the Streamlit application. Utilize Streamlit's data visualization capabilities to create charts and graphs, allowing users to analyze the data effectively.

By following this approach, the Streamlit application serves as a user-friendly interface for interacting with YouTube data, and the combination of MongoDB and MySQL provides a comprehensive solution for handling and analyzing both unstructured and structured data.

**Author: Swapnil Aknurwar**
