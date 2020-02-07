
from ohmysportsfeedspy.v1_1 import API_v1_1
from ohmysportsfeedspy.stores import DataStore


# API class for dealing with v1.2 of the API
class API_v1_2(API_v1_1):

    # Constructor
    def __init__(self, verbose, data_store: DataStore = None):
        super().__init__(verbose, data_store=data_store)

        self.base_url = "https://api.mysportsfeeds.com/v1.2/pull"
