############
# OVERVIEW #
############
"""
Script Name:   Cliflo
Author:        Daniel Risi
Date:          03/04/2019
Status:        Completed
Maintained:    Yes
Overview:      This script uses Selenium to remotely access weather information via Niwas Cliflo database. It also has
               some other nice features such as working out what weather station is closets of a given latitide and
               longitude.
How to use:    TBC
Requirements:  In addition to the below packages you will likley also need to install a webdriver which for Chrome
               can be accessed here: http://chromedriver.chromium.org/downloads
TODO:          - Add method that allows for automatic reset of rows if login falls below 10,000.
"""

############
# PACKAGES #
############

from selenium                   import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from collections                import defaultdict
from sqlalchemy                 import create_engine
from sqlalchemy                 import Table, Column, String, MetaData, select, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm             import sessionmaker
import os
import re
import time
import pandas as pd
import numpy as np

###########
# CLASSES #
###########

class Cliflo:
    """
    Cliflo accesses the NIWA Cliflo database, and preforms a variety of tasks on the website to download data.
    The program relies on selenium and will lilley need to have a webdriver installed to operate properly.
    """

    data_type_dict = {'rainfall': ['Precipitation', "Rain (fixed periods)"],
                      'sunshine_hours': ['Sunshine & Radiation', 'Sunshine'],
                      'temps': ['Temperature and Humidity', 'Max_min_temp'],
                      'soil_moisture': ['Evaporation / soil moisture', 'Soil-moisture'],
                      'evaporation': ['Evaporation / soil moisture', 'Evaporation']
                      }
    # keywords: [{column name: new column name},[list of columns to keep]]
    station_clean_dict = {'rainfall': [{'Amount(mm)': 'amount_mm', 'Station': 'station', 'Deficit(mm)': 'deficit_mm'},
                                       ['amount_mm', 'deficit_mm']],
                          'sunshine_hours': [{'Amount(Hrs)': 'amount_hrs', 'Station': 'station'},
                                             ['amount_hrs']],
                          'soil_moisture': [{'Percent(%)': 'percent', 'Depth(cm)': 'depth_cm', 'Station': 'station'},
                                            ['percent']],
                          'evaporation': [{'Amount(mm)': 'amount_mm', 'Station': 'station'},
                                          ['amount_mm']]}

    def __init__(self, **kwargs):
        self.webdriver = 'C:/webdrivers/chromedriver.exe'
        self.cf_website = 'https://cliflo.niwa.co.nz/'
        self.cf_username = kwargs.get('username', 'NA')
        self.cf_pw = kwargs.get('password', 'NA')
        self.db_name = kwargs.get('db_name', 'sandpit')
        self.db_user_name = kwargs.get('db_user_name', os.environ.get('CLIFLO_USER'))
        self.db_user_pw = kwargs.get('db_user_password', os.environ.get('CLIFLO_USER'))
        self.host = kwargs.get('host',"127.0.0.1")
        self.port = kwargs.get('port', '5432')
        self.diver = kwargs.get('driver', 'psycopg2')
        self.db_type = kwargs.get('db_type', 'postgresql')

        self.db_string = '%s+%s://%s:%s@%s:%s/%s' % (self.db_type,
                                                     self.driver,
                                                     self.db_user_name,
                                                     self.db_user_pw,
                                                     self.host,
                                                     self.port,
                                                     self.db_name)

        self.station_info_table_name = kwargs.get('stations_table_name', 'station_info_test_v2')

        self.engine = create_engine(self.db_string, pool_size=50, max_overflow=0)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.Base = declarative_base()
        # Look up the existing tables from database
        self.Base.metadata.reflect(self.engine)
        self.station_df = pd.DataFrame(columns=['stations'])

    @staticmethod
    def clean_stations(df, start_year, end_year, min_perc_complete):

        df.columns = df.iloc[0]
        df.drop(0, axis=0, inplace=True)
        df.reset_index(inplace=True)
        df.drop(['Select', 'index'], axis=1, inplace=True)
        df['start_day'], df['start_month'], df['start_year'] = \
            df['Start Date'].astype(str).str.split('-', 2).str
        df['end_day'], df['end_month'], df['end_year'] = \
            df['End Date'].astype(str).str.split('-', 2).str

        df.rename(columns={'Lat(dec deg)': 'lat', 'Long(dec deg)': 'long', 'AgentNumber': 'AgentNumber'}, inplace=True)
        cols = ['start_day', 'start_year', 'end_day', 'end_year', 'PercentComplete']
        df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

        # filtering dataframe #
        x_filtered = df[(df['start_year'] <= start_year) &
                        (df['end_year'] >= end_year) &
                        (df['PercentComplete'] >= min_perc_complete)]

        return x_filtered

    def update_lat_long(self, df, station_table_name):

        TableY = type('Table_Y_classname', (self.Base,), {'__tablename__': station_table_name})

        for station in df['AgentNumber']:
            self.session.query(TableY).filter(TableY.stations == str(station)).update(
                {'lat': df.loc[df['AgentNumber'] == station, 'lat'].values[0],
                 'long':df.loc[df['AgentNumber'] == station, 'long'].values[0]})
            self.session.commit()

    def extract_stations(self, **kwargs):
        """
        Searches the list of NIWA weather stations to get stations taht match certain criteia. This list is then used
        to querey the data for each station.
        :param kwargs:
        :return:
        """
        print('extract_stations')
        destination_folder = kwargs.get('to_folder', 'NA')
        data_type = kwargs.get('data_type', 'rainfall')
        start_year = kwargs.get('start_year', 1988)
        end_year = kwargs.get('end_year', 2018)
        min_perc_complete = kwargs.get('min_perc_complete', 100)
        file_name = kwargs.get('file_name', 'station_data_filtered')
        table_name = kwargs.get('station_table_name',  'NA')
        use_file = kwargs.get('use_file', False)

        if use_file:
            df_clean = pd.read_excel(use_file)
        else:
            df = Cliflo.cf_get_stations(self, data_type)
            # Cleaning Dataset #
            df_clean = Cliflo.clean_stations(df, start_year, end_year, min_perc_complete)

            df_clean.to_excel(destination_folder + '\\' + file_name + '.xlsx')

        if not self.engine.dialect.has_table(self.engine, table_name):
            Cliflo.create_stations_table(self, df_clean, start_year, end_year, data_type, table_name)

        Cliflo.update_lat_long(self, df_clean, table_name)

    def cf_get_stations(self, data_type):

        driver, main_window_handle = Cliflo.cf_login(self)
        driver, main_window_handle = Cliflo.cf_specify_data(data_type, driver, main_window_handle)

        driver.switch_to.window(main_window_handle)
        driver.find_element_by_name('agent').click()
        time.sleep(1)
        driver.switch_to.window(driver.window_handles[-1])
        driver.find_element_by_css_selector("input[type='radio'][value='latlongc']").click()
        time.sleep(1)
        driver.find_element_by_name('clat1').clear()
        driver.find_element_by_name('clat1').send_keys(-41)
        time.sleep(1)
        driver.find_element_by_name('clong1').clear()
        driver.find_element_by_name('clong1').send_keys(174)
        time.sleep(1)
        driver.find_element_by_name('crad').clear()
        driver.find_element_by_name('crad').send_keys(1500)
        time.sleep(1)
        driver.find_element_by_name('Submit').click()
        time.sleep(1)
        driver.switch_to.window(driver.window_handles[-1])

        cf_station_df = Cliflo.cf_get_data(driver, data='station_list')

        driver.quit()

        return cf_station_df

    def create_stations_table(self, stations_df, start_year, end_year, data_type, table_name):

        #create dataframe for empty values (as we read yeach years stations value these will change in the database
        # to being not empty
        print('create stations table')
        cols = [data_type +'_'+ str(year) for year in list(range(start_year, end_year+1))] +['lat', 'long']
        df = pd.DataFrame(columns=cols, index=stations_df['AgentNumber'].unique())
        df['stations'] = df.index
        Cliflo.create_table(self, table_name, df, 'stations')
        df.to_sql(table_name, self.engine, if_exists='append', index=False)

    def create_table(self, table_name, df, primary_key_string):
        """

        :param table_name:
        :param df:
        :param primary_key_string:
        :return:
        """

        try:
            print('creating table named: ', table_name)
            meta = MetaData(self.engine)
            primary_key_flags = []
            # create primary key column
            print('table columns: ', df.columns)
            for col_name in df.columns:
                if col_name == primary_key_string:
                    primary_key_flags.append(True)
                else:
                    primary_key_flags.append(False)

            table = Table(table_name, meta,
                          *(Column(col_name,
                                   String,
                                   primary_key=primary_key_flag)
                            for col_name, primary_key_flag in zip(df.columns, primary_key_flags)))
            table.create(self.engine)

            self.Base.metadata.reflect(self.engine)
            primaryKeyColName = table.primary_key.columns.values()[0].name
            print('table created')

        except Exception as e:
            print(e)

    def create_year_list(self, table_name, station_id, start_year, end_year, csv_only, use_existing_data,
                         Table, TableX):
        print('creating year list')
        year_list = list(range(start_year, end_year))

        if (self.engine.dialect.has_table(self.engine, table_name)) and (csv_only == False) and use_existing_data:
            # switch Table connection back on once table has been made (i.e not table - mkes Table False so we make
            # a table then need to switch it back on again
            if Table is False:
                Table = type('Station_Obs_Class', (self.Base,), {'__tablename__': table_name})

            print('creating dict of stations and years already in database...')

            empty_years = []
            [empty_years.append(int(re.sub('[^0-9]', '', key))) for
             key, value in self.session.query(TableX).filter_by(stations=str(station_id)).one().__dict__.items()
             if value == 'empty']

            station_data_dict = defaultdict(list)
            [station_data_dict[int(station_id)].append(int(year[0])) for year in
                 self.session.query(getattr(Table, 'year')).filter_by(station=str(station_id)).distinct() if Table]

            year_list = list((set(list(range(start_year, end_year))) - set(station_data_dict[station_id])
                              - set(empty_years)))

            print('years ' + str(year_list) + ' from station ' + str(station_id) + ' are NOT in DB')

        return year_list

    @ staticmethod
    def file_to_df_if_exists(destination_folder, file_name):
        file = destination_folder + "\\" + file_name
        print('checking file...')
        try:
            df = pd.read_csv(file + '.csv')
            print('yes - csv')
        except:
            try:
                df = pd.read_html(file + 'html')[0]
                print('yes - html')
            except:
                df = False

        return df

    def run_data_update(self, driver, station_id, year, data_type, data_freq, table_name,
                        destination_folder, main_window_handle, use_existing_data, csv_only, TableX, year_list):

        # TODO run to db for all downloaded files html and csv files in a folder
        """
        Method does the following things based on conditions:

        1. If csv file for the data is already downloaded:
            uploads that csv rather than downloading it if user specifies Use Existing if True
        2. If user says Use Exisitng as False or in CSV does not exist the update will download from NIWA the
            sation data.

        :param driver: selnium webdriver
        :param station_id:
        :param year:
        :param data_type: type of data we are getting (rainfall, sunshine hours etc.
        :param data_freq: daily monthly etc (currently can only do daily)
        :param table_name: name of table in database we are updating
        :param destination_folder: name of folder we want to save any new csv downloads
        :param main_window_handle: used in selenium webdriver to access the main page of Cliflo
        :return:
        """
        print('data update - station: ' + str(station_id) + ', year: ' + str(year))
        file_name = 'station ' + str(station_id) + ' - ' + data_type + ' - ' + data_freq + ' (' + str(year) \
                    + ' - ' + str(year + 1) + ')'

        cf_df = Cliflo.file_to_df_if_exists(destination_folder, file_name)

        if year == year_list[0]:
            driver = Cliflo.cf_get_station_data(driver, main_window_handle, station_id)

        # download data from NIWA if csv is not already downloaded or if the downloaded file is empty
        if cf_df is False:
            print('getting data from CliFlo...')
            Cliflo.cf_change_year(driver, main_window_handle, year)
            cf_df = Cliflo.cf_get_data(driver, data='station_obs', data_type=data_type)
            Cliflo.cf_data_to_csv(cf_df, destination_folder, file_name)
            driver.back()

        cf_df = Cliflo.station_obs_quick_clean(cf_df)

        if not cf_df.empty and not csv_only:
            Cliflo.df_to_db(self, cf_df, table_name, data_type, station_id, year, use_existing_data)
            print('sent to database')

        # Storing status of operation in Stations DF so if the entry is empty in future it will not call it.
        output = Cliflo.analyse_cf_data(cf_df, data_type,Cliflo.station_clean_dict[data_type][1][0])
        Cliflo.update_stations_table(self, output, station_id, year, data_type, TableX)

    def update_stations_table(self, entry_status, station_id, year, data_type, TableX):
        print('updating stations table...')
        col_name = data_type + '_' + str(year)
        self.session.query(TableX).filter(TableX.stations == str(station_id)).update({col_name: str(entry_status)})
        self.session.commit()

    def update_data(self, **kwargs):
        """

        :param kwargs:
                station_file: - the file that has the list of stations to search through.
                destiantion_file: where to send the cleaned file.
                data_type: which data you want to extract. options are in the self.data_type_dict

        :return:
        """
        #### USER INPUTS ####
        print('running update')
        station_info = kwargs.get('stations', 'NA')
        destination_folder = kwargs.get('to_folder', 'NA')
        data_type = kwargs.get('data_type', 'rainfall')
        data_freq = kwargs.get('freq','daily')
        start_year = kwargs.get('start_year', 1988)
        end_year = kwargs.get('end_year', 2018)
        table_name = kwargs.get('table_name', 'niwa_rainfall')
        use_existing_data = kwargs.get('use_existing_data', True)
        csv_only = kwargs.get('csv_only', False)
        station_table_name = kwargs.get('station_table_name', 'NA')


       # Get list of staions to query.
        if type(station_info) is str:
            stations = pd.read_excel(station_info)
        else:
            stations = station_info

        station_list = stations['AgentNumber']

        # Connect to NIWA and select the type of data we want.
        driver, main_window_handle = Cliflo.cf_login(self)
        driver, main_window_handle = Cliflo.cf_specify_data(data_type, driver, main_window_handle)
        if self.engine.dialect.has_table(self.engine, table_name):
            Table = type('Station_Obs_Class', (self.Base,),  {'__tablename__': table_name})
        else:
            Table = False

        print(Table)
        TableX = type('Table_X_classname', (self.Base,), {'__tablename__': station_table_name})
        for station_id in station_list:
            #if table exisits, get years not in table for given station
            year_list = Cliflo.create_year_list(self, table_name, station_id, start_year, end_year, csv_only,
                                                use_existing_data, Table, TableX)
            [Cliflo.run_data_update(self, driver, station_id, year, data_type, data_freq, table_name,
                                    destination_folder, main_window_handle, use_existing_data, csv_only, TableX,
                                    year_list)
             for year in year_list if year_list]

        driver.quit()

    def cf_login(self):

        ###############################
        # Logging into Cliflo Website #
        ###############################

        driver = webdriver.Chrome(self.webdriver)
        driver.set_page_load_timeout(100)
        driver.get(self.cf_website)
        driver.find_element_by_name('cusername').send_keys(self.cf_username)
        driver.find_element_by_name('cpwd').send_keys(self.cf_pw)
        time.sleep(1)
        driver.find_element_by_name('submit').click()
        time.sleep(1)
        driver.find_element_by_name('sub_refresh').click()
        time.sleep(1)
        # store current window handle
        main_window_handle = driver.current_window_handle
        driver.switch_to.window(main_window_handle)

        return driver, main_window_handle

    @staticmethod
    def cf_specify_data(data_type, driver, main_window_handle):

        ############################
        # Selecting Metric we want #
        ############################
        driver.switch_to.window(main_window_handle)
        driver.find_element_by_name('datatype2').click()
        driver.switch_to.window(driver.window_handles[-1])
        driver.find_element_by_link_text("Daily and Hourly Observations").click()
        time.sleep(1)
        driver.find_element_by_link_text(Cliflo.data_type_dict[data_type][0]).click()
        time.sleep(1)
        driver.find_element_by_link_text(Cliflo.data_type_dict[data_type][1]).click()
        time.sleep(1)
        driver.switch_to.window(main_window_handle)

        return driver, main_window_handle

    @staticmethod
    def cf_get_station_data(driver, main_window_handle, station_id):

        driver.switch_to.window(main_window_handle)
        driver.find_element_by_name('agent').click()
        time.sleep(0.3)
        # Extract station data
        driver.switch_to.window(driver.window_handles[-1])
        time.sleep(0.3)
        driver.find_element_by_css_selector("input[type='radio'][value='ag']").click()
        time.sleep(0.3)
        driver.find_element_by_name('cAgent').clear()
        time.sleep(0.3)
        driver.find_element_by_name('cAgent').send_keys(station_id)
        time.sleep(0.3)
        driver.find_element_by_name('Submit').click()
        time.sleep(0.3)
        driver.switch_to.window(driver.window_handles[-1])
        time.sleep(1)
        driver.find_element_by_xpath(
            "//input[@name='cstn' and @value='" + str(station_id) + "']").click()
        time.sleep(0.3)
        driver.find_element_by_xpath(
            "//input[@name='Submit' and @value='Replace Selected Stations']").click()
        time.sleep(0.3)
        driver.close()

        return driver

    @staticmethod
    def cf_change_year(driver, main_window_handle, year):
            ##### Selecting Dates we wnat ####

            driver.switch_to.window(main_window_handle)
            driver.find_element_by_name('date1_1').clear()
            driver.find_element_by_name('date1_2').clear()
            driver.find_element_by_name('date1_3').clear()
            driver.find_element_by_name('date1_4').clear()

            time.sleep(1)

            driver.find_element_by_name('date2_1').clear()
            driver.find_element_by_name('date2_2').clear()
            driver.find_element_by_name('date2_3').clear()
            driver.find_element_by_name('date2_4').clear()

            driver.find_element_by_name('date1_1').send_keys(year)
            driver.find_element_by_name('date1_2').send_keys(1)
            driver.find_element_by_name('date1_3').send_keys(1)
            driver.find_element_by_name('date1_4').send_keys(00)

            time.sleep(1)

            driver.find_element_by_name('date2_1').send_keys(year+1)
            driver.find_element_by_name('date2_2').send_keys(1)
            driver.find_element_by_name('date2_3').send_keys(1)
            driver.find_element_by_name('date2_4').send_keys(00)

            time.sleep(1)

            driver.find_element_by_name('submit_sq').click()
            delay = 360  # seconds
            WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.LINK_TEXT, 'CliFlo Home')))

            return driver

    @staticmethod
    def cf_get_data(driver, **kwargs):
        """
        Gets table from cliflo as a data frame then cleans the data frame if it is not empty.
        :param driver:
        :param kwargs:
        :return:
        """
        data = kwargs.get('data', 'NA')
        delay = 1000  # seconds
        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.LINK_TEXT, 'CliFlo Home')))
        time.sleep(0.3)

        if data == 'station_list':
            table = driver.find_element_by_xpath(
                "//form[@action='/pls/niwp/wstn.update_stn_query']/table[1]").get_attribute('outerHTML')

        elif data == 'station_obs':
            table = driver.find_element_by_xpath("//table[3]").get_attribute('outerHTML')

        df = pd.read_html(table)[0]

        return df

    @staticmethod
    def analyse_cf_data(cf_df, data_type, col):

        df_metric = col
        print('analyse metric: ', df_metric)

        if cf_df.empty:
            df_status = 'empty'
        else:
            df = Cliflo.station_obs_preprocess(cf_df, data_type)
            df_status = str(df.loc[df[df_metric] == np.nan, df_metric].count() /
                            len(df[df_metric]))
            print('% empty: ', df_status)

        return df_status

    @staticmethod
    def cf_data_to_csv(df, destination_folder, file_name):

        #check if folder exists otherwise make destination folder
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)
        if not df.empty:
            df.to_csv(destination_folder+'\\'+file_name+'.csv', encoding='utf-8', index=False)

    @staticmethod
    def station_obs_quick_clean(df):
        df.columns = df.iloc[1]
        df.drop([0, 1], axis=0, inplace=True)
        df.reset_index(inplace=True)
        return df

    def df_to_db(self, x, table_name, data_type, station_id, year, use_existing):
        """
        STAGE 1:    Loads html file into a pandas dataframe, checks table exists otherwise creates table.
        STAGE 3:    Only adds dataframe rows to database if those rows are not already in it.

        :param html_file: html_file from Cliflo to be sent to database
        :param table_name: table name of where file will be downloaded.
        :return: nothing:)
        """
        print('running station ' + str(station_id) + ' year ' + str(year) + ' to database')
        status = False
        # will not add empty data frame to database
        if not x.empty:
            # Clean dataframe into a method that allows for analysis
            x = Cliflo.station_obs_preprocess(x, data_type)
            # creates database if not already in
            if not self.engine.dialect.has_table(self.engine, table_name):
                Cliflo.create_table(self, table_name, x, 'rowid')
            if use_existing:
                # Creates a dataframe of values not already in table
                clean_df = Cliflo.clean_df_db_dups(x, table_name, self.engine, dup_cols=['rowid'])
            else:
                pass
            # TODO create finction that removes rows in table so new values can be added in next step
            # Writes data into a database
            clean_df.to_sql(table_name, self.engine, if_exists='append', index=False)
            status = True
            print('uploaded to database sucessfully')
        return status

    @staticmethod
    def clean_df_db_dups(df, tablename, engine, dup_cols=[],
                         filter_continuous_col=None, filter_categorical_col=None):
        """
        Remove rows from a dataframe that already exist in a database
        Required:
            df : dataframe to remove duplicate rows from
            engine: SQLAlchemy engine object
            tablename: tablename to check duplicates in
            dup_cols: list or tuple of column names to check for duplicate row values
        Optional:
            filter_continuous_col: the name of the continuous data column for BETWEEEN min/max filter
                                   can be either a datetime, int, or float data type
                                   useful for restricting the database table size to check
            filter_categorical_col : the name of the categorical data column for Where = value check
                                     Creates an "IN ()" check on the unique values in this column
        Returns
            Unique list of values from dataframe compared to database table
        """
        args = 'SELECT %s FROM %s' % (', '.join(['"{0}"'.format(col) for col in dup_cols]), tablename)
        args_contin_filter, args_cat_filter = None, None
        if filter_continuous_col is not None:
            if df[filter_continuous_col].dtype == 'datetime64[ns]':
                args_contin_filter = """ "%s" BETWEEN Convert(datetime, '%s')
                                              AND Convert(datetime, '%s')""" % (filter_continuous_col,
                                                                                df[filter_continuous_col].min(),
                                                                                df[filter_continuous_col].max())

        if filter_categorical_col is not None:
            args_cat_filter = ' "%s" in(%s)' % (filter_categorical_col,
                                                ', '.join(["'{0}'".format(value) for value in
                                                           df[filter_categorical_col].unique()]))

        if args_contin_filter and args_cat_filter:
            args += ' Where ' + args_contin_filter + ' AND' + args_cat_filter
        elif args_contin_filter:
            args += ' Where ' + args_contin_filter
        elif args_cat_filter:
            args += ' Where ' + args_cat_filter

        df.drop_duplicates(dup_cols, keep='last', inplace=True)
        df = pd.merge(df, pd.read_sql(args, engine), how='left', on=dup_cols, indicator=True)
        df = df[df['_merge'] == 'left_only']
        df.drop(['_merge'], axis=1, inplace=True)
        return df

    @staticmethod
    def station_obs_preprocess(df, data_type):

        print('preprocess')
        #striping out day month and year
        df['year'] = df['Date(NZST)'].str[:4]
        df['month'] = df['Date(NZST)'].str[4:6]
        df['day'] = df['Date(NZST)'].str[6:8]
        for val in Cliflo.station_clean_dict[data_type][1]:
            df[val+'_estimated'] = str(0)

        # Make sure that there is no missing  rows - if so add them.
        period = pd.Period('2018', freq='Y')
        x = pd.Series(pd.DatetimeIndex(start=period.start_time, end=period.end_time, freq='D')).to_frame('date')
        x['year'], x['month'], x['day'] = x['date'].astype(str).str.split('-', 2).str
        z = x.merge(df, how='right')
        z.rename(columns=Cliflo.station_clean_dict[data_type][0], inplace=True)
        z.fillna('-', inplace=True)
        # applying an average for misssing data
        # TODO make this into a multivariate anlaysis as an average is incorrect for rainfall
        for val in Cliflo.station_clean_dict[data_type][1]:
            z.loc[z[val] == '-', val+'_estimated'] = str(1)
        [z[val].replace({'-': np.nan}, inplace=True) for val in Cliflo.station_clean_dict[data_type][1]]
        [z[val].fillna((z[val].median()), inplace=True) for val in Cliflo.station_clean_dict[data_type][1]]
        z['station'].fillna((z['station'].mode()), inplace=True)
        z['rowid'] = data_type + z[['station', 'year', 'month', 'day']].apply(lambda p: ''.join(p), axis=1)
        cols = (['rowid', 'day', 'year', 'month', 'station']
                + [val for val in Cliflo.station_clean_dict[data_type][1]]
                + [val + '_estimated' for val in Cliflo.station_clean_dict[data_type][1]])
        print('preprocess sucessful')
        return z[cols]