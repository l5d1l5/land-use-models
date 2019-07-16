"""

This python file contains all the scripts to take data from various sources and store the data into a postgres database
for future.

The lucm_etl_app.py file calls these files when doing the etl process for various datasets.

Author: Daniel Risi (risi.dj@gmail.com / daniel.risi@mpi.govt.nz)
"""

#################
# Packages used #
#################

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
# Classes #
###########

"""
Current Classes include:
    - Cliflo: takes data from NIWA website and saves it to a folder 
    
"""




#######################################################################################################################

class MaMiroData:

    def __init__(self, **kwargs):

        self.db_name = kwargs.get('db_name', 'sandpit')
        self.db_user_name = kwargs.get('db_user name', "postgres")
        self.db_user_pw = kwargs.get('db_user password', '&MaM!r0postgres&')


        host = "127.0.0.1"
        port = "5432"
        driver = 'psycopg2'
        db_type = 'postgresql'

        self.db_string = '%s+%s://%s:%s@%s:%s/%s' % (db_type,
                                                     driver,
                                                     self.db_user_name,
                                                     self.db_user_pw,
                                                     host,
                                                     port,
                                                     self.db_name)

        self.engine = create_engine(self.db_string, pool_size=50, max_overflow=0)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.Base = declarative_base()
        # Look up the existing tables from database
        self.Base.metadata.reflect(self.engine)
        self.station_df = pd.DataFrame(columns=['stations'])

    def update_data(self, **kwargs):

        #### USER INPUTS ####
        excel_file = kwargs.get('file','NA')
        table_name = kwargs.get('table_name', 'MaMiroData')
        use_existing_data = kwargs.get('use_existing_data', False)
        df = pd.read_excel(excel_file)

        if not self.engine.dialect.has_table(self.engine, table_name):
            create_table(self, table_name, df, 'item')

        if use_existing_data:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)

        print('table_updated')

####### Other functions ###########

# def create_table(self, table_name, df, primary_key_string):
#     """
#
#     :param table_name:
#     :param df:
#     :param primary_key_string:
#     :return:
#     """
#
#     print('creating table: '+table_name)
#     meta = MetaData(self.engine)
#     primary_key_flags = []
#     # create primary key column
#     for col_name in df.columns:
#         if col_name == primary_key_string:
#             primary_key_flags.append(True)
#         else:
#             primary_key_flags.append(False)
#
#     table = Table(table_name, meta,
#                   *(Column(col_name,
#                            String,
#                            primary_key=primary_key_flag)
#                     for col_name, primary_key_flag in zip(df.columns, primary_key_flags)))
#     table.create(self.engine)
#     print('table created')