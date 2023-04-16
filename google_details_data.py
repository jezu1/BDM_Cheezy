import pyarrow as pa
from pyarrow import fs
import pyarrow.parquet as pq
import pandas as pd, numpy as np
import requests
import json
import os


def hdfs_connect():
    hadoop_classpath = os.popen("hadoop classpath --glob").read().strip()
    os.environ['CLASSPATH'] = hadoop_classpath
    hdfs = fs.HadoopFileSystem(host='default', port=0)

    return hdfs


def get_basic_data(hdfs):
    # read all basic data
    file_paths = [f"{file_info.path}" for file_info in
                  hdfs.get_file_info(fs.FileSelector('/user/hadoop/google_rest/', recursive=True))]
    rest_data = pq.ParquetDataset(file_paths, filesystem=hdfs)
    rest_table = rest_data.read()

    # Convert the PyArrow table to a Pandas dataframe
    df_basic = rest_table.to_pandas()

    #remove the duplicates
    df_basic = df_basic.drop_duplicates(subset=['place_id'])

    return df_basic

def save_to_hadoop_as_parquet(hdfs,df_save,filepath):
    pa_table = pa.Table.from_pandas(df_save)
    hdfs_file = hdfs.open_output_stream(filepath)
    pq.write_table(pa_table, hdfs_file)
    hdfs_file.close()


# Main function
def main():
    #connect to hadoop
    hdfs = hdfs_connect()
    df_basic = get_basic_data(hdfs)

    key = 'AIzaSyCDPrhzg6tbQM4-MSZnXoIT3drMgJpo6HY'
    for index, restaurant in df_basic.iterrows():
        # print(restaurant)
        place_id = restaurant['place_id']
        url = "https://maps.googleapis.com/maps/api/place/details/json?place_id=" \
              + str(place_id) + "&fields=opening_hours,photos,reviews&language=en&key=" + key

        respon = requests.get(url)
        jj = json.loads(respon.text)
        # json to dataframe
        df_response = pd.json_normalize(jj['result'])

        if not df_response.empty:
            # put file to hadoop
            hdfs_file_path = '/user/hadoop/google_details/g_' + str(place_id) + '.parquet'
            save_to_hadoop_as_parquet(hdfs,df_response,hdfs_file_path)





# Call main function
if __name__ == '__main__':
    main()