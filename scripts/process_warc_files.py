import pandas as pd
import utils
import os
import argparse
from llama_cpp import Llama

class CCFiles:
    def __init__(self, data_directory) -> None:
        self.data_directory = data_directory
        self.geographies = self._avail_geographies()
        self.all_geographies = self._all_geos()
        self.postcodes_la = self._post_geos()

    def _avail_geographies(self) -> list:
        """
        Get list of available geographies for which we can find and map postcodes
        """
        geos = [f for f in os.listdir(f'{self.data_directory}/data/shapefiles/') if not f.startswith('.')]
        return geos
    
    def _all_geos(self) -> list:
        """
        Get list of all possible UK geographies that we could work on, should the user have a shapefile
        """
        geos_file = pd.read_csv(f'{self.data_directory}/data/geopandas/GBR_adm/GBR_adm2.csv')
        return geos_file['NAME_2'].to_list()

    def _post_geos(self) -> dict: 
        """
        Get dictionaries mapping postcodes and local authorities 
        """
        mapping_file = pd.read_csv(f'{self.data_directory}/data/utility_data/postcodes_LA_mapping.csv')
        mapping_dict = dict(zip(mapping_file['LA'], mapping_file['postcode']))
        return mapping_dict

    def get_html(self) -> pd.DataFrame:
        """
        Get links of webpages within archived website
        """
        df = pd.read_pickle(f'{self.data_directory}/domains.pkl')
        wat_files = utils.wat_getter(df)
        html_df = utils.lambda_getter_html(wat_files)
        df_with_links = utils.lambda_arefs(html_df)
        clean_df_with_links = df_with_links.drop(columns = ['html'])
        return clean_df_with_links.head()

    def text_classifier(self, model_path):
        llm = Llama(model_path = model_path)
        prompt_example = ("Tell me what product this company sells in less than 100 words")
        output = llm(prompt_example, max_tokens=512, echo=True)
        class_ = output["choices"][0]["text"]
        return class_

    def get(self, geography = None, industry_class = False) -> pd.DataFrame:
        """
        Get text of archived webpages, a combination of text and postcodes per geography,
        or industrial classification
        """
        df = pd.read_pickle(f'{self.data_directory}/domains.pkl')
        df = df[0:20]
        #replace the below with function from utils
        df['needed_warc'] = df['needed_warc'].str.replace('/warc/', '/wet/').replace('warc.gz', 'warc.wet.gz')
        df = utils.lambda_getter(df)
        df.dropna(subset = ['text'])
        results_df = df.mask(df.eq('None')).dropna(subset = 'text')  
        if geography:
            geo_postcode = self.postcodes_la.get(geography)                                     #TO-DO: accept both lower and upper case geographies
            results_df = utils.postcode_counter_webpage(results_df, geography = geo_postcode)
        else: 
            results_df = utils.postcode_counter_webpage(results_df)
        if industry_class == True:
            results_df = results_df['text'].apply(utils.text_classifier)     
        return results_df


    def get_and_save(self, df_saving_size):
        df = pd.read_pickle(f'{self.data_directory}/domains.pkl')
        results = pd.DataFrame()
        df_split = np.array_split(df, df_saving_size)
        for df in tqdm(df_split):
            results = pd.concat([results, parallelize_df(df, get)]) 
            results_to_pickle.to_pickle(self.data_directory)
        return results_to_pickle, results 

def main():
    parser = argparse.ArgumentParser(description="CCFiles Utility")
    parser.add_argument("--data-directory", required=True, help="Path to the data directory")
    parser.add_argument("--get-html", action="store_true", help="Call get_html method")
    parser.add_argument("--get", action="store_true", help="Call get method")
    parser.add_argument("--geography", help="Geography argument for get method")
    parser.add_argument("--df_saving_size", help="Defines how often you want to save the results of file processing")
    args = parser.parse_args()

    cc_files_instance = CCFiles(data_directory=args.data_directory)

    if args.get_html:
        result = cc_files_instance.get_html()
    elif args.get:
        result = cc_files_instance.get(geography=args.geography)
    else:
        print("Specify either --get-html or --get option.")
        return


if __name__ == "__main__":
    main()