import geopandas as gpd
import pyarrow as pa
from pyarrow import fs
import pyarrow.parquet as pq
import pandas as pd, numpy as np
from shapely.geometry import Point
import requests
import json
import time
import os

#connect to hadoop file system
def hdfs_connect():
    hadoop_classpath = os.popen("hadoop classpath --glob").read().strip()
    os.environ['CLASSPATH'] = hadoop_classpath
    #set host and port to default from the hadoop config file
    hdfs = fs.HadoopFileSystem(host='default', port=0)

    return hdfs

#divide shapefile to a grid and return the points
def create_grid(shapefile):
    # Get the bounding box of the shapefile
    minx, miny, maxx, maxy = shapefile.total_bounds

    # Create a grid of points with a spacing of 1000 meters
    x_points = np.arange(minx, maxx, 0.0025)
    y_points = np.arange(miny, maxy, 0.0025)
    points = [Point(x, y) for x in x_points for y in y_points]

    return points

#get the coordinates for which the places API needs to be called
def get_bcn_coordinates():
    # get shapefile of barcelona
    barcelona = gpd.read_file(r"Shapefiles_Bcn/shapefiles_barcelona_distrito.shp")
    #get grid points
    points = create_grid(barcelona)

    # create subareas with radius 0.007
    sub_surfaces = []
    for point in points:
        polygon = point.buffer(0.0025)
        sub_surface = gpd.GeoDataFrame({'geometry': polygon}, crs=barcelona.crs, index=[0])
        sub_surfaces.append(sub_surface)

    merged_sub_surfaces = gpd.GeoDataFrame(pd.concat(sub_surfaces, ignore_index=True), crs=barcelona.crs)

    # check if the created subareas intersect with barcelona area
    intersect = []
    for sub_surface in merged_sub_surfaces.iterrows():
        inter = barcelona.intersects(sub_surface[1].geometry).any()
        intersect.append(inter)

    merged_sub_surfaces['intersect'] = intersect
    # remove the subareas that are outside of barcelona
    new_surface = merged_sub_surfaces[(merged_sub_surfaces.intersect == True)]

    #get the centroids of the subareas
    coordinates = []
    for surface in new_surface.iterrows():
        coordinate = str(surface[1].geometry.centroid.y) + ',' + str(surface[1].geometry.centroid.x)
        coordinates.append(coordinate)

    return coordinates

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
    #get coordinates for the api call
    coordinates = get_bcn_coordinates()


    keywords = ['restaurant']
    radius = '200'
    api_key = 'AIzaSyCDPrhzg6tbQM4-MSZnXoIT3drMgJpo6HY'  # insert your Places API
    file_count = 1

    for coordinate in coordinates:
        # in case we need more than just restaurants in the future
        for keyword in keywords:
            #url for the API call
            url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=' + coordinate + '&radius=' + str(
                radius) + '&keyword=' + str(keyword) + '&key=' + api_key
            count = 0
            while True:
                #call the api
                respon = requests.get(url)
                jj = json.loads(respon.text)

                # json response to pandas dataframe
                df_response = pd.json_normalize(jj['results'])

                # check if the response contains results
                if not df_response.empty:
                    # put file to hadoop
                    hdfs_file_path = '/user/hadoop/google_rest/google' + str(file_count) + '.parquet'
                    save_to_hadoop_as_parquet(hdfs, df_response, hdfs_file_path)
                    file_count = file_count + 1

                time.sleep(3)
                #check if there is a next page in the response
                if 'next_page_token' not in jj:
                    #proceed to next call/iteration
                    break
                else:
                    #get the next page
                    next_page_token = jj['next_page_token']
                    url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?key=' + str(
                        api_key) + '&pagetoken=' + str(next_page_token)



if __name__ == '__main__':
    main()