# data-engineering G12
# https://github.com/martmagi/data-engineering
Kaja Jakobson

Rasmus Bobkov

Mart Mägi

Roland Pajuleht


# Our proposed taskplan:
Idea was to make a minimalistic ER schema. Keep all the atomic attributes in respective entitites and keep all the 
cardinalities under control. Meaning that there woulnd't exist M to N pairs.
![img_2.png](https://github.com/martmagi/data-engineering/tree/main/images/img_2.png?raw=true)

we worked out a plan. On the first step, which is data cleaning, we drop the fields which we believe are not necessary for us, so that we can decrease the computational load due to memory footprint and calculations and so on. With the second step, data augmentation/enrichment, we chose to use google vision API response on our url. On the third step, data transformations, the idea was to get more meaningful information from the raw data and finally with the analysis step to run some queries, such as the most popular day for the meme upload or region specific memes and so on. 

# Our proposed ER schema:
![img_20.png](img_20.png)

# Unsuccessful attempts:
Since the coding background for Rasmus, Kaja and Roland was minimal to say the least, this project seemed rather overwhelming in the beginning. Luckily from a database course, which Rasmus was taking in parallel, he was introduced to this tool called talend. It seemed like a user-friendly ETL tool designed specifically for us in mind. An environment where you can simply drag and drop boxes and on the background some Java code does all the heavy lifting for you. After working for many hours on reading up on how to import json and some different working mechanics on boxes and how to connect them and so forth, we got the first pipeline working, everything seemed to go as planned.
However when we wanted to integrate it with the airflow, making a JAR runnable file out of the talend job used, our efforts were in vain. The integration did not work because of the t.jsoninput component used in talend job was not supported by airflow. Below is a screenshot from the first pipeline done with talend.
![img_5.png](img_5.png)

`docker-compose up`

# Pipeline overview
Before moving on to the specific dags themselves, one can see the overall structure of the pipelines as it can be seen in the airflow UI. Starting from the  DAG 0, which is the dag tying up all the following subdags and ending with the dag called run_neo4j, which is the dag for importing our output from the transform dag to neo4_j database. At the bottom work with parallelizing some tasks can be seen.

![img_8.png](img_8.png)
![img_9.png](img_9.png)

# DAG0
Dag 0 is kind of like metadag for our pipeline. From the necessary imports, it can be seen that python operator is mainly used, since it is the most familiar language to us in the group. Following the imports the metadag ties together all the other dags, allowing for a general readpath for subdags, and also the schedule interval for the entire pipeline can be set through that dag. 

![img_11.png](img_11.png)

# DAG1
The first official DAG is downloading the necessary data files. For our pipelines we needed 2 different files. First is the previously mentioned raw data file kym.json and also kym_vision.json file, the google vision API file, which has already been put together on the dataset. 
![img_13.png](img_13.png)

# DAG2

So as for the cleaning, we imported necessary libraries. Then read in the file as a dataframe. First thing, we decided to get the data we wanted out of a dictionary field, details. Then we created new columns for status, origin and year and looped them in. next we dropped unwanted columns, duplicates based on title and url. After that the  decision was to keep only confirmed memes in our dataset. The necessary criteria for that decision was specified in status column. Last step was to write out the json file and that’s all done with the cleaning.
![img_15.png](img_15.png)

# DAG3
Now the output of the previous dag is piped to dag 3, which is the data augmentation dag. Luckily for us we already had the necessary google vision API responses in a file set up. So the task in that regard was more as a data transformation step for us. So first thing to do was to read in both the downloaded file as a dataframe df_vision and cleaned file as df, then get the set difference for all the values between two dataframes based on url overlap. That list was used to drop all the unneccesary records in the df_vision. Then new columns were created for the attributes we wanted to extract from the vision dataframe. 
Next we looped through every url in the dag2 output and extracted the wanted fields where we had url match between the dataframes. Here we wanted to get all the API reponses for the type of content, regarding its classification as of belonging in the adult, spoof, medical, violence or racism categories.
The problem with description attribute from the vision_dataframe was that it was in a list as bunch of items. Since there didn’t seem to be any already built-in method to insert a list into a dataframe cell, a new list with the length of the original dataframe was made. Every url in the original dataframe was looped through and based on url match, up to 10 most popular API responses for the meme description were extracted and written under respective index in the created list. The final step was to insert that newly created and filled list to original dataframe under  column named - google_api_description,  and save it all out as json again.

![img_17.png](img_17.png)

# DAG4
The next stage in the pipeline was the transformations. These ones we decided to keep pretty basic, since we didn’t see really what else we could transform here, since the numeric content of the data was pretty restricted and more sophisticated nlp is over our heads at this point in time. So we had 2 numeric fields we could work with. The first one was the unix timestamp from the time the meme was imported to the knowyourmeme database. So we extracted the human readable datetime from that timestamp. From that datetime we extracted day, month and year. And on top of that we calculated a new value - how_long_till_upload, which gives us the time window in years since the time of meme creation and upload to knowyourmeme database. It works for old memes, but since the origin is given simply as a year for us, then with newer memes its most likely going to be just 0. These newly transformed fields however help us with various meaningful queries.

![img_19.png](img_19.png)

# DAG5
But before that step,  one actually needs something to query upon.
So as for our fifth dag we chose to import it to mongodb using pymongo.
We chose to use mongoDB, since it was introduced to us in a course practice session and this particular session sympathized to us. MongoDB seemed like a perfect fit for nebwies like us. 
![img_22.png](img_22.png)

# MongoDB queries
Here various queries working on our imported data can be seen. For example: how many confirmed memes are created every year? Based on the query response we can see those numbers for the years 2008…2011, since we have limited the response to 5 items in total.

![img_24.png](img_24.png)

and the response

![img_25.png](img_25.png)

Originally we wanted to query if there are any memes about Estonia, but sadly, there were not. So how about our neighbours over the Baltic sea. To our surprise there was only one meme about Finland, which is called Apu apustaja.

![img_26.png](img_26.png)

and the response 

![img_27.png](img_27.png)


Here are the top 5 tags, which are the most common as over all the memes in our dataset.

![img_29.png](img_29.png)

and the response

![img_30.png](img_30.png)


Top 5 places where most of the memes originate from.
![img_31.png](img_31.png)

and the response

![img_32.png](img_32.png)

And now using some field generated through our data transformation, here are the 6 most meme heavy months.

![img_34.png](img_34.png)

and the response 

![img_35.png](img_35.png)


# Contributions of members to the project

In large the project was done as a group effort with us having regularly scheduled workout times, so that we booked a time and worked throughout those ours over the zoom.
Even as such, the tasks completed per person were roughly as follows:

Mart - did most of the heavy coding required with airflow integrations and to get it properly running.
Roland - Worked on Talend and ER-schema
Kaja - Worked on Queries, Talend, ER-schema and project management
Rasmus - Worked on pipelines, presentation and project management
