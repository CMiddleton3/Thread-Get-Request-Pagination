import requests
import json
import threading

class GetApiPagination:
    def __init__(self, endpoint, page_size=500, headers=None, total_count_keyname=None,data_field='data'):
        """
        Initialize the class with the API endpoint, page size, page number, header, and field name for total count.

        :param endpoint: API endpoint to retrieve pagination data from.
        :type endpoint: str
        :param page_size: Number of items per page (default is 100).
        :type page_size: int
        :param page: Starting page number (default is 1).
        :type page: int
        :param header: Header for API request (default is None).
        :type header: dict
        :param total_count_field: Field name for the total count of items in the API response (default is None).
        :type total_count_field: str
        """
        self.endpoint = endpoint
        self.page_size = page_size
        self.headers = headers
        self.total_count_keyname = total_count_keyname
        self.total_count = 0
        self.data_field = data_field
        self.results = []
        self.lock = threading.Lock()
        
        self._pull_api()

    def add_header(self, header_key, header_value):
        """
        Add a header to the API request.

        :param header: Header for API request.
        :type header: dict
        """
        if self.headers is None:
            self.headers = {}
        self.headers[header_key] = header_value

    def set_data_field(self, data_field):
        self.data_field = data_field
    
    def set_total_count_keyname(self,total_count_keyname):
        self.total_count_keyname = total_count_keyname

    def get_results(self):
        """
        Get the combined pagination data as a single JSON.

        :return: Combined pagination data.
        :rtype: dict
        """
        # for t in self.threads:
        #     t.join()

        return json.dumps(self.results)
    
    def _get_total_count(self, data):
        """
            Get the total count of items from the API response.

            :param data: JSON data from the API response.
            :type data: dict
            :return: Total count of items.
        """
        
        if 'total_count' in data:
            total_count_keyname = data['total_count']
        elif 'totalCount' in data:
            total_count_keyname = data['totalCount']
        elif 'count' in data:
            total_count_keyname = data['count']
        else:
            raise KeyError("Key for total count not found in response data")

        return total_count_keyname

    def save_to_file(self, filename):
        """
        Save the combined pagination data as a JSON file.

        :param file_path: File path to save the combined pagination data.
        :type file_path: str
        """
        with open(filename, 'w') as file:
            json.dump(self.results, file)

    def refresh_api(self):
        self.results = []
        self._pull_api(self)

    def _pull_api(self):
        self.threads = []

        if self.total_count_keyname is None:
            response = requests.get(self.endpoint, headers=self.headers)
            if response.status_code == 200:
                data = response.json()

                try:
                    self.total_count =self._get_total_count(self, data)
                except KeyError as e:
                    raise e
        else:
            if self.total_count_keyname in data:
                self.total_count = data[self.total_count_keyname]
            else:
                raise KeyError("Key for total count %s not found in response data" % self.total_count_keyname)

        for i in range(0, self.total_count, self.page_size):
            t = threading.Thread(target=self._download_results, args=(i,))
            t.start()
            self.threads.append(t)
            
            for t in self.threads:
                t.join()

    def _download_results(self, start):
        sub_response = requests.get(self.endpoint + "&pageSize=" + str(self.page_size) + "&page=" + str(start // self.page_size), headers=self.headers)
        data = sub_response.json()[self.data_field] # Data = result set
        if sub_response.status_code == 200:
            with self.lock:
                self.results.extend(data)