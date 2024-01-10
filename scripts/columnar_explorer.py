import pandas as pd
import json
import gzip
import os 
from tqdm import tqdm 
import requests
import os
import numpy as np 

class ColumnarExplorer: 
    def __init__(self, monthly_path) -> None: 
        self.monthly_path = monthly_path
        self.schema = self._fetch_schema()
        self.monthly_urls = self._get_monthly_indices()

    def _fetch_schema(self):
        """
        Get schema of Common Crawl columnar format 
        """
        schema = 'https://github.com/commoncrawl/cc-index-table/blob/main/src/main/resources/schema/cc-index-schema-flat.json'
        resp = requests.get(schema)

        return json.loads(resp.text)

    def _get_monthly_indices(self):
        """
        Extract indices to access monthly dumps of the crawl 
        """
        index_paths = requests.get(self.monthly_path)
        open('monthly_index.gz', 'wb').write(index_paths.content)
        links_for_download_cdx = []
        with gzip.open('monthly_index.gz', 'rb') as f:
            for line in f:
                links_for_download_cdx.append(line)
        os.remove('monthly_index.gz')
        links_for_download_cdx_strings = [str(byte_string, 'UTF-8').rstrip('\n') for byte_string in links_for_download_cdx]
        links_to_get_indices = ['https://data.commoncrawl.org/' + x for x in links_for_download_cdx_strings] 
        links_to_get_indices = links_to_get_indices[0:6]

        return(links_to_get_indices)

    def get_domain(self, domain: str, chunks: int, clean = False):   #set default n of chunks as 1
        """
        Get monthly records of a user defnined domain (e.g. .com)
        """
        links_to_get_indices = self.monthly_urls
        chunks_of_indices = numpy.array_split(links_to_get_indices, chunks)
        results = pd.DataFrame()

        for chunk in chunks_of_indices:
            print(chunk)
            urls = pd.DataFrame()
            for link_of_indices in tqdm(chunk):
                link_to_download = 'curl -o columnar.gz.parquet ' + ''.join(link_of_indices) 
                os.system(link_to_download)
                df = pd.read_parquet('columnar.gz.parquet', engine='fastparquet')
                data = df.loc[df['url_host_private_suffix'] == domain] 
                print('number of found f'{domain}' in this chunk is')
                print(len(data))
                urls = pd.concat([urls, data], axis=0).reset_index(drop=True)
                print(len(urls))
                os.remove('columnar.gz.parquet')

                if clean:
                    urls = urls.drop(['url_surtkey', 'url_host_tld', 'url_host_2nd_last_part', 'url_host_3rd_last_part', 
                                    'url_host_4th_last_part', 'url_host_5th_last_part', 'url_host_name_reversed', 'url_protocol', 
                                    'url_port', 'url_path', 'url_query', 'content_digest', 'content_mime_detected'], axis = 1) 	
                    urls = urls.loc[(urls['fetch_status'] == 200)]
                    urls = urls[~urls.warc_filename.str.contains('robotstxt')]  

                else: 
                    urls = urls 
            
        results = pd.concat([results, urls])
        return results

def main():
    parser = argparse.ArgumentParser(description="ColumnarExplorer Utility")
    parser.add_argument("--monthly_path", required=True, help="Path to the data directory")
    parser.add_argument("--domain", action="store_true", help="Select specific domain to collect")
    parser.add_argument("--clean", action="store_true", help="Constrain the dataset for ease of storage")
    args = parser.parse_args()

    columnar_explorer_instance = ColumnarExplorer(data_directory=args.data_directory)

    result = columnar_explorer_instance.get_doman()

if __name__ == "__main__":
    main()