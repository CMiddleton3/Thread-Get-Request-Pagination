import requests
import json
import threading

class GetApiPagination:
    def __init__(self, endpoint, page_size=100, headers=None, total_count=None):
        self.endpoint = endpoint
        self.page_size = page_size
        self.headers = headers
        self.total_count = total_count
        self.results = []
        self.lock = threading.Lock()
        self._pull_api()

    def add_header(self, header_key, header_value):
        if self.headers is None:
            self.headers = {}
        self.headers[header_key] = header_value

    def get_results(self):
        for t in self.threads:
            t.join()

        return json.dumps(self.results)

    def save_to_file(self, filename):
        with open(filename, 'w') as file:
            json.dump(self.results, file)

    def _pull_api(self):
        self.threads = []

        if self.total_count is None:
            response = requests.get(self.endpoint, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                self.total_count = data.get('total_count', data.get('totalCount', data.get('count', 0)))
        else:
            self.total_count = self.total_count

        for i in range(0, self.total_count, self.page_size):
            t = threading.Thread(target=self._download_results, args=(i,))
            t.start()
            self.threads.append(t)

    def _download_results(self, start):
        sub_response = requests.get(self.endpoint + "&pageSize=" + str(self.page_size) + "&page=" + str(start // self.page_size), headers=self.headers)
        if sub_response.status_code == 200:
            with self.lock:
                self.results += sub_response.json()['results']