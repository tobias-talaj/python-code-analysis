import pickle
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


class PythonLibraryScraper:
    def __init__(self, max_depth=2):
        self.visited = set()
        self.result_dict = {}
        self.max_depth = max_depth
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

    def visit_page(self, url, depth=0):
        """
        Visit a given URL, parse its content, and collect information about Python libraries.
        
        This method is recursive and visits linked pages up to a specified depth. It collects
        information about classes, methods, functions, attributes, and exceptions from the
        official Python documentation.

        Args:
            url (str): The URL to visit and parse.
            depth (int, optional): The current depth of the recursive call. Defaults to 0.

        Side Effects:
            1. Makes network requests to fetch the contents of the provided URL. The number
            of requests depends on the depth and the number of internal links encountered.
            2. Modifies the `visited` set and `result_dict` dictionary within the
            `PythonLibraryScraper` class instance, which keeps track of visited URLs and
            accumulates information about Python libraries during the scraping process.
            3. Uses recursion to traverse the internal links found in the visited web pages.

        Returns:
            None
        """

        if url in self.visited or depth > self.max_depth:
            return

        self.visited.add(url)

        response = self.session.get(url, headers=self.headers)
        html = response.content
        soup = BeautifulSoup(html, 'html.parser')

        internal_links = soup.find_all('a', {'class': 'reference internal'})
        if depth == 0:
            for link in internal_links:
                link_url = urljoin(url, link['href'])
                self.visit_page(link_url, depth + 1)
            return

        if not any(h1.find('a', {'class': 'reference internal'}) for h1 in soup.find_all('h1', {'class': None})):
            return
        else:
            lib_name_element = soup.find('h1', {'class': None}).find('a', {'class': 'reference internal'})
            lib_name = lib_name_element.text

        if lib_name not in self.result_dict:
            self.result_dict[lib_name] = {
                'class': [],
                'method': [],
                'function': [],
                'attribute': [],
                'exception': []
            }

        dl_class_map = {
            'py attribute': ('attribute', 'span.sig-name.descname'),
            'py class': ('class', 'span.sig-name.descname'),
            'py method': ('method', 'span.sig-name.descname'),
            'py function': ('function', 'span.sig-name.descname'),
            'py exception': ('exception', 'span.sig-name.descname')
        }

        for dl_class, (dict_key, span_selector) in dl_class_map.items():
            dls = soup.find_all('dl', {'class': dl_class})
            for dl in dls:
                values = {span.text.strip() for span in dl.select(span_selector)}
                if values:
                    self.result_dict[lib_name][dict_key].extend(values)
                    self.result_dict[lib_name][dict_key] = list(set(self.result_dict[lib_name][dict_key]))

        for key_to_remove in ['method', 'attribute']:
            if key_to_remove in self.result_dict[lib_name] and 'class' in self.result_dict[lib_name]:
                self.result_dict[lib_name]['class'] = [cls for cls in self.result_dict[lib_name]['class'] if cls not in self.result_dict[lib_name][key_to_remove]]

    def save_result(self, file_path):
        with open(file_path, 'wb') as f:
            pickle.dump(self.result_dict, f)


if __name__ == '__main__':
    index_url = 'https://docs.python.org/3/library/index.html'
    scraper = PythonLibraryScraper()
    scraper.visit_page(index_url)
    scraper.save_result('standard_library_api_dict.pickle')
