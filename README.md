# MLOpsReviews

Welcome to my **MLOps** project where I deploy an API on **Render** using the **fastAPI** framework. This API contains 6 end points with queries performed across the data from the four platforms and the 7th endpoint is part of a content recommendation model for movies.

In this repository, you will find 2 jupyter files named 'cleaning' and 'EDA' where I prepare all the necessary data to be consumed by the API.

The clean data consumed by fastAPI can be found in the files 'data_api.csv' and 'data_ML.csv'.

All the 7 endpoints can be found in the 'main.py' file. Unfortunately Render does not provide enough memory for the last endpoint to run properly, but it does work locally.

Link to the API deployed on Render: https://mlopsreviews.onrender.com/docs

Link to Google Drive with the raw datasets: https://drive.google.com/drive/folders/1iTgUo9KpCBnJq6XMWg-WxqGahTY6vWX-?usp=share_link
