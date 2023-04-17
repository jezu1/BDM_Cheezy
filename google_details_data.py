import pyarrow as pa
from pyarrow import fs
import pyarrow.parquet as pq
import pandas as pd
import requests
import json
import os

#connect to hadoop file system
def hdfs_connect():
    hadoop_classpath = os.popen("hadoop classpath --glob").read().strip()
    os.environ['CLASSPATH'] = hadoop_classpath
    # set host and port to default from the hadoop config file
    hdfs = fs.HadoopFileSystem(host='default', port=0)

    return hdfs

#get the basic data that was retrieved by the places API nearby search call from hadoop
def get_basic_data(hdfs):
    # get all the data from the hadoop directory
    file_paths = [f"{file_info.path}" for file_info in
                  hdfs.get_file_info(fs.FileSelector('/user/hadoop/google_rest/', recursive=True))]
    rest_data = pq.ParquetDataset(file_paths, filesystem=hdfs)
    rest_table = rest_data.read()

    # Convert the PyArrow table to a Pandas dataframe
    df_basic = rest_table.to_pandas()

    #remove the duplicates (as the subareas from the places API call can overlap we might have duplicates)
    df_basic = df_basic.drop_duplicates(subset=['place_id'])

    return df_basic

#transform json file to parquet and save to hadoop
def save_to_hadoop_as_parquet(hdfs,df_save,filepath):
    pa_table = pa.Table.from_pandas(df_save)
    hdfs_file = hdfs.open_output_stream(filepath)
    pq.write_table(pa_table, hdfs_file)
    hdfs_file.close()


# Main function
def main():
    #connect to hadoop
    hdfs = hdfs_connect()
    #get data from google places api call
    df_basic = get_basic_data(hdfs)

    key = 'AIzaSyCDPrhzg6tbQM4-MSZnXoIT3drMgJpo6HY'

    #for every restaurant that was retrived by the places api call, call the places details api
    # to get  the opening_hours, photos (references) and reviews of the restaurant
    for index, restaurant in df_basic.iterrows():

        place_id = restaurant['place_id']
        url = "https://maps.googleapis.com/maps/api/place/details/json?place_id=" \
              + str(place_id) + "&fields=opening_hours,photos,reviews&language=en&key=" + key

        #call the details API
        respon = requests.get(url)
        jj = json.loads(respon.text)
        # json response to pandas dataframe
        df_response = pd.json_normalize(jj['result'])

        if not df_response.empty:
            # put file to hadoop
            hdfs_file_path = '/user/hadoop/google_details/g_' + str(place_id) + '.parquet'
            save_to_hadoop_as_parquet(hdfs,df_response,hdfs_file_path)





# Call main function
if __name__ == '__main__':
    main()