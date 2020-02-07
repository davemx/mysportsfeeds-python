#!/usr/bin/env python

# Version 2.1.0

# Install:
# git clone https://github.com/mysportsfeeds/mysportsfeeds-python.git
# cd /PATH/TO/DIRECTORY/
# sudo python setup.py install

# Usage:
# Data_query = MySportsFeeds('1.2', verbose=True)
# Data_query.authenticate('YOUR_API_KEY', 'YOUR_ACCOUNT_PASSWORD')
# Output = Data_query.msf_get_data(league='nba',season='2016-2017-regular',feed='player_gamelogs',format='json',player='stephen-curry')

from ohmysportsfeedspy.v1_0 import API_v1_0
from ohmysportsfeedspy.v1_1 import API_v1_1
from ohmysportsfeedspy.v1_2 import API_v1_2
from ohmysportsfeedspy.v2_0 import API_v2_0
from ohmysportsfeedspy.v2_1 import API_v2_1
from ohmysportsfeedspy.stores import DataStore, FileStore, validate_data_store, DEFAULT_FILE_STORE_DIRECTORY


class MySportsFeeds(object):
    """ Main class for all interaction with the MySportsFeeds API """

    # Constructor
    def __init__(self, version='1.2', verbose=False, data_store: DataStore = FileStore(DEFAULT_FILE_STORE_DIRECTORY),
                 store_type=None, store_location=None):
        self.__verify_version(version)

        data_store = validate_data_store(data_store, store_type, store_location)

        self.version = version
        self.verbose = verbose

        # Instantiate an instance of the appropriate API depending on version
        if self.version == '1.0':
            self.api_instance = API_v1_0(self.verbose, data_store)

        if self.version == '1.1':
            self.api_instance = API_v1_1(self.verbose, data_store)

        if self.version == '1.2':
            self.api_instance = API_v1_2(self.verbose, data_store)

        if self.version == '2.0':
            self.api_instance = API_v2_0(self.verbose, data_store)

        if self.version == '2.1':
            self.api_instance = API_v2_1(self.verbose, data_store)

    # Make sure the version is supported
    def __verify_version(self, version):
        if version != '1.0' and version != '1.1' and version != '1.2' and version != '2.0' and version != '2.1':
            raise ValueError("Unrecognized version specified."
                             "Supported versions are: '1.0', '1.1', '1.2', '2.0', '2.1'")

    # Authenticate against the API (for v1.0)
    def authenticate(self, apikey, password):
        if not self.api_instance.supports_basic_auth():
            raise ValueError("BASIC authentication not supported for version " + self.version)

        self.api_instance.set_auth_credentials(apikey, password)

    # Request data (and store it if applicable)
    def msf_get_data(self, **kwargs):
        return self.api_instance.get_data(**kwargs)

