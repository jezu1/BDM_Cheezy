# -*- coding: utf-8 -*-
"""
Code to move semi-cleaned data from hdfs dump to delta lake tables
"""
import os, json
import pandas as pd
import pyspark
from delta import configure_spark_with_delta_pip

# Set up hadoop
hadoop_classpath = os.popen("hadoop classpath --glob").read().strip()
os.environ['CLASSPATH'] = hadoop_classpath
hdfs_prefix='hdfs://localhost:9000'

# Set up spark
# https://towardsdatascience.com/hands-on-introduction-to-delta-lake-with-py-spark-b39460a4b1ae

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

def get_files(hdfs_prefix):
    """
    Get list of all csv/json files in hdfs that need to be processed to delta lake
    """
    files=[]
    p=os.popen("hdfs dfs -ls /user/hadoop/landing").read()
    folders=['/user/'+i.split('/user/')[-1] for i in p.split('\n') if '/user/' in i and 'hadoop' in i]

    for f in folders:
        p=os.popen(f"hdfs dfs -ls {f}").read()
        files.append(['/user/'+i.split('/user/')[-1] for i in p.split('\n') if i.endswith(('csv','json','parquet'))])

    files=sum(files,[])
    files=[hdfs_prefix+i for i in files]
    return files

def clean_google(df):
    """
    Drop unnecessary columns from google datasets
    """
    drop_cols=['icon', 'icon_background_color','icon_mask_base_uri','reference',
               'geometry.viewport.northeast.lat','geometry.viewport.northeast.lng',
               'geometry.viewport.southwest.lat','geometry.viewport.southwest.lng',
               'types','scope', 
               'photos',
               'opening_hours.open_now',
               'plus_code.compound_code' 'plus_code.global_code'
               ]
    try:
        return df.drop(columns=[i for i in drop_cols if i in df.columns])
    except:
        return df

def save_to_delta(df, savepath):
    """
    Create delta tables from pandas tables and save into hdfs
    """
    # schema=get_spark_schema(df))\ 
    spark.createDataFrame(df)\
                        .write.format("delta")\
                        .mode("overwrite")\
                        .save(savepath)
    print('Saved file in hadoop: '+savepath)
                        
def read_save_to_delta(file):
    """
    Read files and save as delta table
    """
    # Create path where to save in deltalake
    savepath=file.replace('landing','delta').replace('.csv','/').replace('.json','/').replace('.parquet','/')
    
    if file.endswith('json'):
        # just load some sample json file and put it to pandas dataframe
        with open(file) as f:
            # Load the contents of the file into a variable
            data = json.load(f)
        for key in data.keys():
            print(key)
            savepath=savepath+f'{key}/'
            df = pd.json_normalize(data[key])
            save_to_delta(df, savepath)
            
    elif file.endswith('.parquet'):
        df = pd.read_parquet(file)
        # For google datasets, execute only for those that are operational
        try:
            if df['business_status'][0] == 'OPERATIONAL':
                df=clean_google(df)
                save_to_delta(df, savepath)
        except:
            pass
        
    elif file.endswith('csv'):
        # Check if there is an index column
        if pd.read_csv(file, nrows=0).columns[0]=='Unnamed: 0':
            df=pd.read_csv(file, index_col=0)
        else: 
            df=pd.read_csv(file)
        save_to_delta(df, savepath)

for file in get_files(hdfs_prefix):
    read_save_to_delta(file)
