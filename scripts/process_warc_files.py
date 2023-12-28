import pandas as pd
import utils
import os
import argparse

class CCFiles:
    def __init__(self, data_directory) -> None:
        self.data_directory = data_directory
        self.geographies = self._avail_geographies()
        self.all_geographies = self._all_geos()

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

    def get(self, geography = None, text = True, industry_class = False) -> pd.DataFrame:
        """
        Get text of archived webpages, a combination of text and postcodes per geography,
        or industrial classification
        """
        df = pd.read_pickle(f'{self.data_directory}/domains.pkl')
        df = utils.lambda_getter(df)
        df.dropna(subset = ['text'])
        df_text = df.mask(df.eq('None')).dropna(subset = 'text')  
        results_df = utils.postcode_counter_webpage(df_text, geography = geography)
        return print(results_df)

def main():
    parser = argparse.ArgumentParser(description="CCFiles Utility")
    parser.add_argument("--data-directory", required=True, help="Path to the data directory")
    parser.add_argument("--get-html", action="store_true", help="Call get_html method")
    parser.add_argument("--get", action="store_true", help="Call get method")
    parser.add_argument("--geography", help="Geography argument for get method")
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


