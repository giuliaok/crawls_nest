# crawls_nest: guide for future contributors! 

- [TO-DO list](#to-do-list)
- [ColumnarExplorer](#columnarexplorer)
  	- [Rationale](#rationale)
	- [Attributes](#attributes)
   	- [Parallelization](#parallelization)
- [CCFiles](#ccfiles)
  - [Rationale](#rationale)
  	- [Text classification](#text-classification)
  - [Attributes](#attributes)
  - [Parallelization](#parallelization)
  

The workflow is structured over 2 main Classes: ``ColumnarExplorer`` and ``CCFiles``. 

- ``ColumnarExplorer`` select CommonCrawl columnar files for a user defined month and year
- ``CCFiles`` extracts text from CommonCrawl and processes it

## TO-DO list 


A TO-DO list containing missing features and bugs can be found [`here`](https://github.com/giuliaok/crawls_nest/issues/1)

ColumnarExplorer
--------------------

### Rationale

For most crawls of the archive (2014 onwards), we can use the provided index to WARC files and URLs in a columnar format. This is substantially a dataframe looking something like:

| url_surtkey | url | url_host_name | url_host_tld | url_host_2nd_last_part | ... | content_languages | warc_filename | warc_record_length |
|----------|----------|----------|----------|----------|----------|----------|----------|----------|
| com,wordpress,examplewebsite)/   | https://examplewebsite.wordpress.com/  | examplewebsite.wordpress.com | com  | wordpress  | ... | eng  | crawl-data/CC-MAIN-2022-33/segments/1659882572... | 	16771  |



The script for ColumnarExplorer extracts files in columnar format for a user defined monh and year. It is currently self contained and can be found in [`columnar_explorer.py`](https://github.com/giuliaok/crawls_nest/blob/main/scripts/columnar_explorer.py). 

Each crawl contains approximately 900 parquet files in columnar format, which cana be downloaded through sending a request to the *monthly_urls* attribute (see below). The files come as a list in the following format: 

```ruby
['https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2022-33/subset=warc/part-00299-d466b69e-be2b-4525-ac34-1b10d57329da.c000.gz.parquet',
 'https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2022-33/subset=warc/part-00298-d466b69e-be2b-4525-ac34-1b10d57329da.c000.gz.parquet',
 'https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2022-33/subset=warc/part-00297-d466b69e-be2b-4525-ac34-1b10d57329da.c000.gz.parquet',
 'https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2022-33/subset=warc/part-00296-d466b69e-be2b-4525-ac34-1b10d57329da.c000.gz.parquet', ... ]
```

Each one of these parquet files is around 2 GB in size, and depending on one's broadband connetion speed might take several minutes to download. As saving the complete files locally would occupy a ridiculous amount of memory, our pipeline avoids this by using the ```get_domain``` function in [`columnar_explorer.py`](https://github.com/giuliaok/crawls_nest/blob/main/scripts/columnar_explorer.py) as follows: 

1. Initialise an empty dataframe
2. Download the parquet file through ``curl``
3. Open the file and slice it according to one's research need
4. Only save in memory the essential features of the columnar file (e.g. webpage name, WARC location)
5. Append to the initialised dataframe and repeat for each file

We experimented on *.co.uk* webpages, finding approximately 2 million webpages stored for each archive's segment, occupying ~900MB in memory. Doable :smiley: 

### Attributes

The class has got 3 main attributes: 

- *schema*: outputs the name of the features available in each columnar file. This is useful in case you wished to extract a specific subset of the crawl, e.g. webpages in Tagalog
- *months_years*: for each available crawl, it outputs a list of dictionaries containing the name of the crawl, the year and the month, as in the snippet below. The list of dictionary has been extracted by operatiing some simple manipulation on CommonCrawl's documnentation pages. 
  
```ruby
{'name': 'CC-MAIN-2023-50', 'month': 'November/December', 'year': '2023'}
```
- *monthly_ulrs*: based on the user defined month and year of choice, it prepares the url to download the data for a crawl's chunk, download its contents and formats them to be later used in order to download columnar format files. This principal url for download should look like:

```ruby
https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-33/cc-index-table.paths.gz
```

### Parallelization

It is not yet completely clear to me whether the file downloading process would benefit from parallelization, as it is highly affected by a user's broadband download speed. This hypothesis still needs to be tested and has been flagged in the [`TO-DO list`](https://github.com/giuliaok/crawls_nest/issues/1) :innocent:

ColumnarExplorer

CCFiles
--------------------


### Rationale

The ``CC_files`` class carries out one main operation: extracting text or hyperlinks from a webpage's record. The class is contained in the [`process_warc_files.py`](https://github.com/giuliaok/crawls_nest/blob/main/scripts/process_warc_files.py) and heavily relies on functions written within the [`utils.py`](https://github.com/giuliaok/crawls_nest/blob/main/scripts/utils.py) and [`utils_html.py`](https://github.com/giuliaok/crawls_nest/blob/main/scripts/utils_html.py) scripts. Texts are extracted using the --get argument while html using the --get_html argument. To extract both text and hyperlinks we rely on ```warcio```. Functions for extracting these two features for a webpage from an archival record are contained in the ```utils.py``` and ```utils_html.py``` scripts. Both functions, ```text_getter``` and ```html_getter``` respectively, work as follows: 

1. Convert the relevant warc file in a ``wet`` file for text and in a ``wat`` file for hyperlinks
2. Request the link of the relevant file. We use the ``Session`` object in requests which allows to persist certain parameters across requests.
3. We then process the response obtained for the file. **This response is an archival file containing on average ~25.000 webpage records. It does NOT only contain the record of the target webpage.** The file hence needs to be iterated upon to find the taget webpage, and this is done through the warcio class ```ArchiveIterator```.
   - Within ``wet`` files, text is contained within the ``conversion`` record type. To understand the characteristics of this type of record, we suggest you have a look at the [`WARC format documentation`](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/)
   - Within ``wat`` files, hyperlinks are found within the ``metadata`` record type.
4. Sometimes this might not work! This might be because the initial request has gone wrong, but we are not completely sure of what else might break archival requests. In that case, we catch the exception and proceede to the next webpage. 

  
The argument ```--get``` allows users to extract text from webpages and apply some functions over it. Currently The purpose of these functions is twofold: 

1. Find and extract postcodes
   - This can be done for a full country or for a specific geography
2. Classify webpages by product sold
   - We would like this to be done either for single webpages or for a full website. At the moment it is done for single webpages. 

When calling [`process_warc_files.py`](https://github.com/giuliaok/crawls_nest/blob/main/scripts/process_warc_files.py) a user might decide to extract postcodes (and for which geography), do text classification, or do both. 

:exclamation: When I started working with warc format data, I found [`this tutorial`](https://skeptric.com/notebooks/WAT%20WET%20WARC%20-%20Common%20Crawl%20Archives.html) extremely useful. It is very simple but covers all the basics 

#### Text Classification 

To allow for accurate and **relatively** fast text classification on the spot we use a quantized version of [`Llama`](https://ai.meta.com/llama/), [`llama_cpp`]. We use it in its chat form, whereby for each webpage text we ask a prompt of the type *what is content of this webbpage about?*. Optimally, we would like the user to be able to define both the prompt to feed to the model (according to their research needs), as well as their language model of choice.  

### Attributes

The class has currently got 3 attributes, all related to geography:

- *geographies*: output the geographies for which we currently have collected a shapefile. This might serve in the future for plotting (still to be implemented...)
- *all_geographies*: lists all local authorities in the UK having a distinct postcode. Users can chose a geography for postcode search from within this list. 
- *postcodes_la*: returns the initials of each localy authority's postcode. This is used in the script when searching for postcodes in a user defined geography.

### Parallelization

Warc file processing works way faster when parallelised. We provide code to parallelise the ```get``` and ```get_html``` function through multiprocessing. The ```get_and_save``` function applies the parallelism and additonally saves the results dataframe every *n* files processed. The number of files processed before saving should be defined by the user. We highly suggest using this parameter as the script might run for days and you would not want to lose all that work. Moreover, this allows you to work on the already processed data while the script continues to run.  
