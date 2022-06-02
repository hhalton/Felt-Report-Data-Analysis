"""Update sensitivity map with new earthquake data from Geonet API, if any
   exists.
   Author: Hazel Halton
   Date: 10th - 23rd May 2022 
   """

from urllib.request import urlopen
import json
import numpy as np
import pandas as pd
from calculating_distance import Haversine


LEN_OF_NZ = 1500

def preprocessing(old_quakes_df):
    """Collecting data from Geonet and finding out if there are any new 
    earthquakes to add to the list."""
    quake_df = import_data("quake?MMI=5")
    filtered_quake_df = \
        extract_data(quake_df, 'properties.publicID','geometry.coordinates')
    updated_quakes_df = compare_quake_lists(filtered_quake_df, old_quakes_df)
    return updated_quakes_df

def import_data(prompt):
    """Import data from Geonet API and return a dataframe.  This function uses 
    url requests, based on example queries from the Geonet API page. The data 
    returned is a JSON file which did not produce usable data when converted to
    a dataframe using json_normalise. Following extraction of the 'features' 
    key, the resulting list is converted with json_normalise into a data frame.
    """
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

def compare_quake_lists(filtered_quake_df, old_quakes_df):
    """Compares the stored list of quakes with the new list, removing any
    existing quakes from the new list."""
    quake_list = list(old_quakes_df['properties.publicID'])
    for quake in quake_list:
        index_names = \
            filtered_quake_df[(filtered_quake_df['properties.publicID'] == quake)].index
        filtered_quake_df.drop(index_names, inplace = True)
    return filtered_quake_df


def new_data(updated_data_df, old_data_df, filename):
    """Add new data to stored list."""
    old_data_df = \
        pd.concat([old_data_df, updated_data_df], \
                  ignore_index=True)   
    old_data_df.to_csv(filename)
    return updated_data_df

def processing(updated_quakes_df):
    """Extract the Felt Report data for each new quake and calculate sensitivity
    factor.  Then prepare the data for uploading to ArcGIS."""
    final_intensity_df = get_intensity_data(updated_quakes_df)
    validated_distances_df = distance_validation(final_intensity_df)
    new_sensitivity_df = calculate_sensitivity(validated_distances_df)
    old_sensitivity_df = pd.read_csv("final_sensitivity.csv")
    new_data(new_sensitivity_df, old_sensitivity_df, "final_sensitivity.csv")
    coordinates_list = list(new_sensitivity_df['geometry.coordinates'])
    arcgis_initial_df = collate_arcgis_data(coordinates_list, new_sensitivity_df)
    return arcgis_initial_df    

def get_intensity_data(updated_quakes_df):
    """ This function iterates through the dataframe column of quakes returned 
    by the Geonet API. It uses their PublicID code to extract the intensity and 
    location of Felt Reports completed by members of the public across New 
    Zealand. It then calls the calculate_distance function and adds the 
    resulting values to the final dataframe"""
    final_intensity_df = pd.DataFrame()
    for i in range(len(updated_quakes_df)):
        quake_id = updated_quakes_df['properties.publicID'][i]
        quake_location = updated_quakes_df['geometry.coordinates'][i]
        intensity_df = \
            import_data("intensity?type=reported&publicID=" + quake_id)
        if len(intensity_df) > 0:
            filtered_intensity_df = \
                extract_data(intensity_df, 'geometry.coordinates', 'properties.mmi')
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

def collating_results(arcgis_initial_df):
    """Combines existing ArcGIS file with new data and removes any duplicates.
    """
    old_arcgis_df = pd.read_csv("final_arcgis_data.csv")
    old_arcgis_df = pd.concat([old_arcgis_df, arcgis_initial_df], \
                      ignore_index=True)
    new_arcgis_df = remove_duplicates(old_arcgis_df)
    return new_arcgis_df


def remove_duplicates(old_arcgis_df):
    """Remove duplicate data for locations, leaving only the highest sensitivity
    factor."""
    old_arcgis_df.sort_values(by=['Sensitivity'], inplace=True)
    old_arcgis_df.drop_duplicates(subset=['Longitude', 'Latitude'], keep='last',\
                              inplace=True)
    return old_arcgis_df


def main():
    """The main function."""
    old_quakes_df = pd.read_csv("list of quakes.csv")
    updated_quakes_df = preprocessing(old_quakes_df) 
    if len(updated_quakes_df) == 0:
        print("There are no new quakes to add.")
    else:
        print("Please wait while the new data is collected and processed.")
        updated_quakes_df = updated_quakes_df.reset_index(drop=True)
        new_data(updated_quakes_df, old_quakes_df, "list of quakes.csv")     
        arcgis_initial_df = processing(updated_quakes_df)
        arcgis_final_df = collating_results(arcgis_initial_df)
        arcgis_final_df.to_csv("final_arcgis_data.csv")
        
main()

