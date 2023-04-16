from pyarrow import fs
import pyarrow.parquet as pq
import pandas as pd
import requests
import os
import io
from google.cloud import vision
from google.oauth2.service_account import Credentials

def hdfs_connect():
    hadoop_classpath = os.popen("hadoop classpath --glob").read().strip()
    os.environ['CLASSPATH'] = hadoop_classpath
    hdfs = fs.HadoopFileSystem(host='default', port=0)

    return hdfs

def get_details_data(hdfs):
    file_paths = [f"{file_info.path}" for file_info in
                  hdfs.get_file_info(fs.FileSelector('/user/hadoop/google_details/', recursive=True))]
    df_list = []
    id_list = []
    for path in file_paths:
        table = pq.read_table(path, filesystem=hdfs)
        df = table.to_pandas()
        p_id = path.split('g_')[1].split('.parquet')[0]

        df_list.append(df)
        id_list.append(p_id)

    df_details = pd.concat(df_list)
    df_details['place_id'] = id_list
    df_details = df_details.head(100)

    return df_details


# Main function
def main():

    hdfs = hdfs_connect()
    creds = Credentials.from_service_account_file('optimal-sentry-306318-64acfd5814eb.json')
    client = vision.ImageAnnotatorClient(credentials=creds)

    df_details = get_details_data(hdfs)

    key = 'AIzaSyCDPrhzg6tbQM4-MSZnXoIT3drMgJpo6HY'
    for index, restaurant in df_details.iterrows():

        place_id = restaurant['place_id']
        try:
            df_photo = pd.json_normalize(restaurant['photos'])
            food_count = 1

            if not df_photo.empty:
                for index, photo in df_photo.iterrows():
                    photo_url = 'https://maps.googleapis.com/maps/api/place/photo?' + \
                                'photoreference=' + photo['photo_reference'] + \
                                '&sensor=false&maxheight=' + str(photo['height']) + \
                                '&maxwidth=' + str(photo['width']) + \
                                '&key=' + key

                    print(photo_url)

                    if food_count > 3:
                        break
                    else:
                        response = requests.get(photo_url)
                        image = vision.Image(content=response.content)

                        # Performs label detection on the image file
                        response_vision = client.label_detection(image=image)
                        labels = response_vision.label_annotations

                        for label in labels:
                            if label.description == 'Food':
                                # move to hadoop
                                hdfs_file_path = '/user/hadoop/google_images/g_' + str(place_id) + '_' + str(food_count) + '.jpg'

                                with hdfs.open_output_stream(hdfs_file_path) as stream:
                                    stream.write(io.BytesIO(response.content).read())
                                    stream.flush()
                                    stream.close()

                                food_count = food_count + 1
                                break

        except:
            print('no photos')




# Call main function
if __name__ == '__main__':
    main()