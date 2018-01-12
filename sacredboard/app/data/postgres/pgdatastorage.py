'''
Created on 26.12.2017

@author: sebas
'''
import csv
import os

import psycopg2

from sacredboard.app.data.datastorage import DataStorage,\
    DummyMetricsDAO
from sacredboard.app.data.metricsdao import MetricsDAO
from sacredboard.app.data.rundao import RunDAO
from sacredboard.app.data.postgres.pgrundao import PostgresRunDAO
    
class PostgresDataAccess(DataStorage):
    '''
    Access to records in Postgres Database
    '''
    def __init__(self, server, database_name, port, credentials_filepath, user=None, pw=None):
        '''
        Initialize data accessor.
        '''
        self.server = server
        self.database_name = database_name
        self.port = port
        # set credentials
        if user is not None and pw is not None:
            self.credentials = dict(user=user,pw=pw)
        elif credentials_filepath is not None:
            with open(os.path.expanduser(credentials_filepath), 'r') as cred_file:
                csv_dict = csv.DictReader(cred_file)
                credentials = [elem for elem in csv_dict]
                if len(credentials) != 1:
                    raise ValueError('SOmething is wrong with the credentials file')
                else:
                    self.credentials = credentials[0]
        else:
            raise ValueError('You need to either supply a username and a password or supply a credentials file path.')

    def get_metrics_dao(self) -> MetricsDAO:
        """
        Return a data access object for metrics.

        By default, returns a dummy Data Access Object if not overridden.
        Issue: https://github.com/chovanecm/sacredboard/issues/62

        :return MetricsDAO
        """
        return DummyMetricsDAO()

    def get_run_dao(self) -> RunDAO:
        """
        Return a data access object for Runs.

        :return: RunDAO
        """
        return PostgresRunDAO(self)
    
    def get_db_connection(self, db_name=None):
        if db_name is None:
            db_name = self.database_name
        dbcon = psycopg2.connect(database=db_name,
                                 user=self.credentials['user'],
                                 password=self.credentials['pw'],
                                 host=self.server,
                                 port=self.port)
        return dbcon
        