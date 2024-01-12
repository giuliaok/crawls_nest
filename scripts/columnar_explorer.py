import pandas as pd
import json
import gzip
import os 
from tqdm import tqdm 
import requests
import os
import numpy as np
from bs4 import BeautifulSoup 
import re

class ColumnarExplorer: 
    def __init__(self, month, year) -> None: 
        self.schema = self._fetch_schema()
        self.month = month
        self.year = year 
        self.months_years = self._month_year_parser()
        self.data_parser = self._parse_data(
            month=self.month, year=self.year,
        )
        self.all_paths = self._get_all_paths()
        self.monthly_urls = self._get_monthly_indices()
             

    def _fetch_schema(self):
        """
        Get schema of Common Crawl columnar format 
        """
        schema = 'https://github.com/commoncrawl/cc-index-table/blob/main/src/main/resources/schema/cc-index-schema-flat.json'
        resp = requests.get(schema)

        return json.loads(resp.text)

    def _get_all_paths(self):
        """
        Extract the paths of all crawls. Manually available at "https://www.commoncrawl.org/get-started
        """
        url = "https://www.commoncrawl.org/get-started"
        response = requests.get(url).content
        soup = BeautifulSoup(response, 'html.parser')
        heading_ultras_elements = soup.find_all('h6', class_='heading-ultras')

        elements = []
        for element in heading_ultras_elements:
            elements.append(element.text)
            
        return elements

    def _month_year_parser(self):
        """
        Construct a dictionary of the type {'name': 'CC-MAIN-2023-50', 'month': 'November/December', 'year': '2023'} for each 
        segment of the crawl. Should take under 5 seconds. 
        """
        url = "https://www.commoncrawl.org/get-started"
        response = requests.get(url).content
        soup = BeautifulSoup(response, 'html.parser')
        heading_ultras_elements = soup.find_all('h6', class_='heading-ultras')

        elements = []
        for element in heading_ultras_elements:
            elements.append(element.text)

        crawl_urls = []
        for element in elements: 
            crawl_description_url = 'https://data.commoncrawl.org/crawl-data/' + element + '/index.html'
            crawl_urls.append(crawl_description_url)    

        dates = []
        for url in crawl_urls: 
            resp = requests.get(url).content
            b_soup = BeautifulSoup(resp, 'html.parser')
            p_tag = b_soup.find('p')
            if p_tag:
                p_tag_text = p_tag.text.strip()
                match = re.search(r'the\s+(.*?)\s+crawl', p_tag_text, re.IGNORECASE)
                if match:
                    result = match.group(1)
                dates.append(result)        
        dates[-3:] = ['None None'] *3   
        dictionary = dict(zip(elements, dates))

        result_list = []
        for key, value in dictionary.items():
            name = key
            month, year = value.split(' ')
            result_dict = {'name': name, 'month': month, 'year': year}
            result_list.append(result_dict)

        return result_list

    def _parse_data(self, month, year):
        """
        Use the month_year_extractor to define date of choice 
        """
        data = self.months_years
        for dict in data:
            if dict['year'] == self.year and self.month in dict['month']:
                extracted_name = dict['name']
                print(extracted_name)
                break
        else:
            raise ValueError(f"No matching entry found for {month} {year}")
        return extracted_name 


    def _get_monthly_indices(self):
        """
        Extract indices to access monthly dumps of the crawl, template of needed url is 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-33/cc-index-table.paths.gz' 
        """
        print('start')
        CC_INDEX_SERVER = 'https://data.commoncrawl.org/'
        DATA_ACCESS_PREFIX = CC_INDEX_SERVER + 'crawl-data/'
        DATA_ACCESS_SUFFIX = '/cc-index-table.paths.gz'
        INDEX_NAME = self.data_parser
        INDEX_PATHS = DATA_ACCESS_PREFIX+INDEX_NAME+DATA_ACCESS_SUFFIX
        print(INDEX_PATHS)

        index_paths = requests.get(INDEX_PATHS)
        print(index_paths)
        open('monthly_index.gz', 'wb').write(index_paths.content)
        links_for_download_cdx = []
        with gzip.open('monthly_index.gz', 'rb') as f:
            for line in f:
                links_for_download_cdx.append(line)
        os.remove('monthly_index.gz')
        links_for_download_cdx_strings = [str(byte_string, 'UTF-8').rstrip('\n') for byte_string in links_for_download_cdx]
        links_to_get_indices = [CC_INDEX_SERVER + x for x in links_for_download_cdx_strings] 
        links_to_get_indices = links_to_get_indices[0:6]

        return(links_to_get_indices)

    def get_domain(self, domain: str, chunks: int, cleanse = False):   #set default n of chunks as 1
        """
        Get monthly records of a user defnined domain (e.g. .com)
        """
        links_to_get_indices = self.monthly_urls
        chunks_of_indices = np.array_split(links_to_get_indices, chunks)
        results = pd.DataFrame()

        for chunk in chunks_of_indices:
            print(chunk)
            urls = pd.DataFrame()
            for link_of_indices in tqdm(chunk):
                link_to_download = 'curl -o columnar.gz.parquet ' + ''.join(link_of_indices) 
                os.system(link_to_download)
                df = pd.read_parquet('columnar.gz.parquet', engine='fastparquet')
                data = df.loc[df['url_host_private_suffix'] == domain] 
                #print('number of found f'{domain}' in this chunk is')
                print(len(data))
                urls = pd.concat([urls, data], axis=0).reset_index(drop=True)
                print(len(urls))
                os.remove('columnar.gz.parquet')

                if cleanse:
                    urls = urls.drop(['url_surtkey', 'url_host_tld', 'url_host_2nd_last_part', 'url_host_3rd_last_part', 
                                    'url_host_4th_last_part', 'url_host_5th_last_part', 'url_host_name_reversed', 'url_protocol', 
                                    'url_port', 'url_path', 'url_query', 'content_digest', 'content_mime_detected'], axis = 1) 	
                    urls = urls.loc[(urls['fetch_status'] == 200)]
                    urls = urls[~urls.warc_filename.str.contains('robotstxt')]  

                else: 
                    urls = urls 
            
        results = pd.concat([results, urls])
        return results

import argparse

def main():
    parser = argparse.ArgumentParser(description="ColumnarExplorer Utility")
    parser.add_argument("--month", required=True, help="Month")
    parser.add_argument("--year", required=True, help="Year")
    parser.add_argument("--domain", action="store_true", help="Select specific domain to collect")
    parser.add_argument("--cleanse", action="store_true", help="Constrain the dataset for ease of storage")
    args = parser.parse_args()

    # initialise columnar explorer object
    columnar_explorer_instance = ColumnarExplorer(
        data_directory=args.data_directory,
        month=args.month,
        year=args.year,
    )

    result = columnar_explorer_instance.get_doman(
        domain=args.domain, 
        cleanse=args.cleanse
    )
    return result 

if __name__ == "__main__":
    main()