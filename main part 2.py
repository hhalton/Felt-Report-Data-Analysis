"""Upload data from first interaction with Geonet API which was lost as some
earthquakes occurred more than 365 days prior to later interactions and were 
therefore no longer accessible.
   Author: Hazel Halton
   Date: 4th -  10th May 2022 
   """

import numpy as np
import pandas as pd


def import_data():
    """Import data from both sets API requests and concatenate into one file."""
    sensitivity_df = pd.read_csv('sensitivity.csv')
    sensitivity_original_df = pd.read_csv('sensitivity_original.csv')
    full_sensitivity_df = pd.concat([sensitivity_df, sensitivity_original_df], \
              ignore_index=True)    
    return full_sensitivity_df

def remove_duplicates(full_sensitivity_df):
    """Remove duplicate data for locations, leaving only the highest sensitivity
    factor."""
    full_sensitivity_df.sort_values(by=['sensitivity'], inplace=True)
    full_sensitivity_df.drop_duplicates(subset=['geometry.coordinates'], \
                                        keep='last', inplace=True)
    return full_sensitivity_df

def convert_string(final_sensitivity_df):
    """Converts coordinates from strings to 2 float values."""
    locations = list(final_sensitivity_df['geometry.coordinates'])
    final_location = []
    for location in locations:
        location_2 = location.replace("[", "")
        location_3 = location_2.replace("]", "")
        location_4 = location_3.split(", ")
        location_4 = list(map(float, location_4))
        final_location.append(location_4) 
    return final_location

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


def collate_arcgis_data(coordinates_list, full_sensitivity_df):
    """Collect longitude, latitude and sensitivity data in one dataframe for 
    use with ArcGIS Pro software."""
    arcgis_df = pd.DataFrame()
    arcgis_df['Longitude'] = separate_long_lat(coordinates_list, 0)
    arcgis_df['Latitude'] = separate_long_lat(coordinates_list, 1)
    sensitivity_list = list(full_sensitivity_df['sensitivity'])
    arcgis_df['Sensitivity'] = sensitivity_list
    arcgis_valid_df = longitude_validation(arcgis_df)
    return arcgis_valid_df


def main():
    """The main function."""
    full_sensitivity_df = import_data()
    final_sensitivity_df = remove_duplicates(full_sensitivity_df)
    coordinates_list = convert_string(final_sensitivity_df)
    arcgis_df = collate_arcgis_data(coordinates_list, full_sensitivity_df)
    final_sensitivity_df.to_csv("final_sensitivity.csv")
    arcgis_df.to_csv("final_arcgis_data.csv")    
    
main()