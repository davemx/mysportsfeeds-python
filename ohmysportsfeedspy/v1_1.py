
from ohmysportsfeedspy.v1_0 import API_v1_0
from ohmysportsfeedspy.stores import DataStore


# API class for dealing with v1.1 of the API
class API_v1_1(API_v1_0):

    # Constructor
    def __init__(self, verbose, data_store: DataStore = None):
        super().__init__(verbose, data_store=data_store)

        self.base_url = "https://api.mysportsfeeds.com/v1.1/pull"
