############
# OVERVIEW #
############
"""
Script Name:   My_Data
Author:        Daniel Risi
Date:          03/04/2019
Status:        In Progress
Maintained:    Yes
Overview:      This script contains classes used to extract and analyse data that I have accumulated for this project.
How to use:    TBC
Requirements:  TBC
TODO:          NA
"""

############
# PACKAGES #
############

import os
import re
import time
import pandas as pd
import numpy as np

###########
# CLASSES #
###########


class MyData:
    def __init__(self, **kwargs):

        self.db_name = kwargs.get('db_name', 'sandpit')
        self.db_user_name = kwargs.get('db_user name', os.environ.get('POSTGRES_USER'))
        self.db_user_pw = kwargs.get('db_user password', os.environ.get('POSTGRES_PW'))


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
        excel_file = kwargs.get('file', 'NA')
        table_name = kwargs.get('table_name', 'MaMiroData')
        use_existing_data = kwargs.get('use_existing_data', False)
        df = pd.read_excel(excel_file)

        if not self.engine.dialect.has_table(self.engine, table_name):
            create_table(self, table_name, df, 'item')

        if use_existing_data:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)

        print('table_updated')