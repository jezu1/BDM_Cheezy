# DBM_Cheesy

## Table of Contents
1. [General Info](#general-info)
2. [Set up and requirements](#technologies)
3. [Python scripts](#installation)

### General Info
***
This git repository is part of the university project P1 Landing Zone implemented in the course of Big Data Management at UPC barcelona. The python scripts move data needed for our own startup idea Cheezy to a temporal and persistent landing zone.

## Set up and requirements
***
A virtual machine on OpenNebula with the following specifications is used for this project: 
50 gb GB and OS - Ubuntu 18.04.3 LTS

Other technologies used within the project:
* Hadoop File System: Version 3.3.2 
* Python: Version 3.9.7
* Spark: Version 3.3.0
* Delta Lake: Version 2.1.0 

## Python scripts
***
In the following is briefly described which function which of the python scripts has:

* google_basic_data.py: Divide the area of barcelona into subareas and get for all the subareas the restaurants in that area using the nearby search of the google places API.
* google_details_data.py: Get detailed information for every found restaurant by sending a details request to the google places API.
* google_images.py: Call the photo service of the google places API and perform label detection on the returned images with the google vision API.
* landing_zone.py: Code to transferr local dump for kaggle, tripadvisor, and cheezy data to hdfs.
* persistent_landing_zone.py: Code to convert data in the landing zone to delta tables to be save also in hdfs.
* simulate_cheezy_data.py: Create synthetically data for cheezy application. outputs one json file to be saved locally which contains 6 different tables: user, resto, image, dish, swipes, and location.
* tripadvisior_webscrape.py: Wbscrape barcelona tripdvisor using python selenium and save to local.
