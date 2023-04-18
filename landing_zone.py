# -*- coding: utf-8 -*-
"""
Code to move data from local to hadoop as landing zone
"""
import os

# Save entire data dump directory to hadoop (for tripadvisor, kaggle, and cheesy data)
os.popen("hdfs dfs -put /home/hadoop/Notebooks/dump/ /user/hadoop/landing")

# Check output
p=os.popen("hdfs dfs -ls /user/hadoop/landing").read()
print('Files moved to HDFS:\n','\n'.join(p.split('\n')))