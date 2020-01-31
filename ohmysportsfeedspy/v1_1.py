
from ohmysportsfeedspy.v1_0 import API_v1_0


# API class for dealing with v1.1 of the API
class API_v1_1(API_v1_0):

    # Constructor
    def __init__(self, verbose, store_type=None, store_location=None):
        super().__init__(verbose, store_type, store_location)

        self.base_url = "https://api.mysportsfeeds.com/v1.1/pull"
