"""Produce a heat map showing which areas of New Zealand are more sensitive to
   earthquakes based on Felt It? reports data from Geonet.
   Author: Hazel Halton
   Date: 17th March - 7th April 2022 
   """

from urllib.request import urlopen
import json
import numpy as np
import pandas as pd
from calculating_distance import Haversine


LEN_OF_NZ = 1500


def import_data(prompt):
    """Import data from Geonet API and return a dataframe.  This function uses 
    url requests, based on example queries from the Geonet API page. The data 
    returned is a JSON file which did not produce usable data when converted to
    a dataframe using json_normalise. After printing out the file contents, it 
    could be seen that the first dictionary has only two keys 'type' and 
    'features' of which 'type' contains 1 string and 'features' contains all the
    rest of the data in a list. Following extraction of the 'features' key, the 
    resulting list could be converted with json_normalise into a data frame."""
    url = "http://api.geonet.org.nz/"+ prompt
    response = urlopen(url)
    data_json = json.loads(response.read())
    data_list = data_json.get('features')
    data_df = pd.json_normalize(data_list)
    return data_df


def extract_data(data_df, first_column, second_column):
    """Extract required information from pandas dataframe. There are 
    several columns in the data from the Geonet API, of which only two were 
    needed. This function extracts these columns into a new dataframe."""
    columns = data_df.columns
    filtered_data_df = data_df[[first_column, second_column]].copy()
    return filtered_data_df


def get_intensity_data(filtered_quake_df):
    """ This function iterates through the dataframe column of quakes returned 
    by the Geonet API. It uses their PublicID code to extract the intensity and 
    location of Felt Reports completed by members of the public across New 
    Zealand. It then calls the calculate_distance function and adds the 
    resulting values to the final dataframe"""
    final_intensity_df = pd.DataFrame()
    for i in range(len(filtered_quake_df)):
        quake_id = filtered_quake_df['properties.publicID'][i]
        quake_location = filtered_quake_df['geometry.coordinates'][i]
        intensity_df = \
            import_data("intensity?type=reported&publicID=" + quake_id)
        filtered_intensity_df = \
            extract_data(intensity_df, 'properties.mmi','geometry.coordinates')
        filtered_intensity_df['distance'] = \
            calculate_distance(filtered_intensity_df, quake_location)
        final_intensity_df = \
            pd.concat([final_intensity_df, filtered_intensity_df], \
                      ignore_index=True)
    return final_intensity_df  


def calculate_distance(filtered_intensity_df, quake_location):
    """Calls a Haversine class from calculate_distance file which uses the 
    Haversine equation to calculate the distance between two longitude/
    latitude coordinates.  The function uses the location of the earthquake and 
    that of the Felt Report and returns a list of distances in km.""" 
    distance = []
    for i in range(len(filtered_intensity_df)):
        felt_location = filtered_intensity_df['geometry.coordinates'][i]
        distance.append(Haversine(quake_location, felt_location).km)
    return distance


def distance_validation(final_intensity_df):
    """ Checks that calculated distances (and therefore coordinates) are valid
    by comparing them with the total length of New Zealand and removing any
    rogue rows from the dataframe."""
    index_names = \
        final_intensity_df[(final_intensity_df['distance'] > LEN_OF_NZ)].index
    final_intensity_df.drop(index_names, inplace = True)
    return final_intensity_df


def calculate_sensitivity(validated_distances_df):
    """Calculates the 'sensitivity' factor for each location by multiplying the 
    felt intensity by the distance from the earthquake location and adding
    the value to a new 'sensitivity' column."""
    validated_distances_df['sensitivity'] = \
        validated_distances_df['properties.mmi'] * \
        validated_distances_df['distance']
    return validated_distances_df


def separate_long_lat(coordinates_list, index_num):
    """Separate longitude and latititude coordinates into individual columns."""
    new_coordinate_list = []
    for i in range(len(coordinates_list)):
        new_coordinate_list.append(coordinates_list[i][index_num])
    return new_coordinate_list

def longitude_validation(arcgis_df):
    """ Checks that longitudinal coordinates are valid for New Zealand and 
    removes any rogue rows from the dataframe."""
    index_names = \
        arcgis_df[(arcgis_df['Longitude'] < 0)].index
    arcgis_df.drop(index_names, inplace = True)
    return arcgis_df


def collate_arcgis_data(coordinates_list, sensitivity_df):
    """Collect longitude, latitude and sensitivity data in one dataframe for 
    use with ArcGIS Pro software."""
    arcgis_df = pd.DataFrame()
    arcgis_df['Longitude'] = separate_long_lat(coordinates_list, 0)
    arcgis_df['Latitude'] = separate_long_lat(coordinates_list, 1)
    sensitivity_list = list(sensitivity_df['sensitivity'])
    arcgis_df['Sensitivity'] = sensitivity_list
    arcgis_valid_df = longitude_validation(arcgis_df)
    return arcgis_valid_df


def remove_duplicates(arcgis_df):
    """Remove duplicate data for locations, leaving only the highest sensitivity
    factor."""
    arcgis_df.sort_values(by=['Sensitivity'], inplace=True)
    arcgis_df.drop_duplicates(subset=['Longitude', 'Latitude'], keep='last',\
                              inplace=True)
    return arcgis_df


def main():
    """The main function."""
    quake_df = import_data("quake?MMI=5")
    filtered_quake_df = \
        extract_data(quake_df, 'properties.publicID','geometry.coordinates')
    filtered_quake_df.to_csv("list of quakes.csv")
    final_intensity_df = get_intensity_data(filtered_quake_df)
    validated_distances_df = distance_validation(final_intensity_df)
    sensitivity_df = calculate_sensitivity(validated_distances_df)
    coordinates_list = list(sensitivity_df['geometry.coordinates'])
    arcgis_initial_df = collate_arcgis_data(coordinates_list, sensitivity_df)
    arcgis_final_df = remove_duplicates(arcgis_initial_df)
    sensitivity_df.to_csv("sensitivity.csv")
    arcgis_final_df.to_csv("arcgis_data.csv")    
    
main()