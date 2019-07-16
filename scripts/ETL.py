import Cliflo as CF
import os

extract_data = 'Cliflo'
extract = True #Means it will extrat data from NIWA.
key_val1 = 'sunshine_hours' # options include rainfall, soil_moisture, evaporation, sunshine_hours
start_year = 1987
end_year = 2018

# file dict specifies the folder name, the station list name, the obs data_table and the station info data table.

file_dict = {'rainfall': ['cliflo_rainfall_station_data',
                          'cliflo_rainfall_station_list',
                          'cliflo_rainfall_daily_test2',
                          'cliflo_rainfall_station_info_table_test3',
                          1987, 2018],
             'sunshine_hours': ['cliflo_sunshine_station_data',
                                'cliflo_sunshine_station_list',
                                'cliflo_sunshine_hours_daily_test1',
                                'cliflo_sunshine_hours_station_info_table_test3'],
             'soil_moisture': ['cliflo_soil_moisture_test',
                               'cliflo_soil_moisture_station_list',
                               'cliflo_soil_moisture_daily_test1',
                               'cliflo_moisture_station_info_table_test3'],
             'evaporation': ['cliflo_evaporation_data',
                             'cliflo_evaporation_station_list',
                             'cliflo_evaporation_daily_test1',
                             'cliflo_evaporation_station_info_table_test3']
             }


def main():

    for key_val, value in file_dict.items():
        path_results = 'C:\\Users\\risid\\Google Drive\\Python\\land-use-models\\data\\' + file_dict[key_val][0]
        path_stations = 'C:\\Users\\risid\\Google Drive\\Python\\land-use-models\\data\\station_lists'

        if extract_data == 'Cliflo':
            
            #Set up cliflo session with username and password
            cliflo = CF.Cliflo(username=os.environ.get('CLIFLO_USER'),
                               password=os.environ.get('CLIFLO_PW'),
                               db_name='sandpit', # optional if wanting to extract data to a database
                               db_user_name=os.environ.get('POSTGRES_USER'),
                               db_user_password=os.environ.get('POSTGRES_PW'))

            if extract:
                # Works out what stations we want to use from Cliflo                
                cliflo.extract_stations(to_folder=path_stations, # where the data goes
                                        file_name=file_dict[key_val][1], #csv file name of stations 
                                        data_type=key_val, # type of data we want the stations for i.e rainfall
                                        start_year=start_year, # beginning year we want to make sure is included in the station data
                                        end_year=end_year, # as above but the last year.
                                        min_perc_complete=100, # percentage complete of the data in the CLIflo dataset. 
                                        station_table_name=file_dict[key_val][3], # only relevant if putting values into SQL
                                        use_file=path_stations+'\\'+file_dict[key_val][1] + '.xlsx') # if this is false it will query Clifo directty but if you already have a csv of ths stations you want use that :)

            cliflo.update_data(stations=path_stations + '\\' + file_dict[key_val][1] + '.xlsx',
                               to_folder=path_results,
                               data_type=key_val,
                               freq='daily',
                               update_table=True,
                               table_name=file_dict[key_val][2],
                               use_existing_data=True,
                               csv_only=False, # IMPORTANT - MAKE TRUE TO ONLY EXTRACT DATA TO CSV FORMAT AND NOT TO A DATABASE TOO.
                               start_year=start_year,
                               end_year=end_year,
                               station_table_name=file_dict[key_val][3])

        # elif extract_data == 'MaMiro':
        #     mmd = ETL.MaMiroData(db_name='sandpit',
        #                          db_user_name='postgres',
        #                          db_user_password='&MaM!r0postgres&')
        # 
        #     mmd.update_data(file='C:\\Users\\risid\\Google Drive\\Python\\land-use-models\\data\\mamiro_cost_estimates\\pricing_database.xlsx',
        #                     table_name='mamirodata2')

if __name__ == "__main__":
    main()
