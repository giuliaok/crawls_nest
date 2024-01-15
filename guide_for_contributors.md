crawls_nest: guide for future contributors! 
========================================

The workflow is structured over 2 main Classes: ``ColumnarExplorer`` and ``CCFiles``. 

- ``ColumnarExplorer`` select CommonCrawl columnar files for a user defined month and year
- ``CCFiles`` extracts text from CommonCrawl and processes it

TO-DO list 
--------------------

A TO-DO list containing missing features and bugs can be found [`here`](https://github.com/giuliaok/crawls_nest/issues/1)

ColumnarExplorer
--------------------

For most crawls of the archive (2014 onwards), we can use the provided index to WARC files and URLs in a columnar format. This is substantially a dataframe looking something like:

| Header 1 | Header 2 | Header 3 | Header 4 | Header 5 | Header 6 | Header 7 | Header 8 | Header 9 |
|----------|----------|----------|----------|----------|----------|----------|----------|----------|
| Data 1   | Data 2   | Data 3   | Data 4   | Data 5   | Data 6   | Data 7   | Data 8   | Data 9   |



The script for ColumnarExplorer extracts files in columnar format for a user defined monh and year. It is currently self contained and can be found in [`columnar_explorer.py`](https://github.com/giuliaok/crawls_nest/blob/main/scripts/columnar_explorer.py). 

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


```ruby
['https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2022-33/subset=warc/part-00299-d466b69e-be2b-4525-ac34-1b10d57329da.c000.gz.parquet',
 'https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2022-33/subset=warc/part-00298-d466b69e-be2b-4525-ac34-1b10d57329da.c000.gz.parquet',
 'https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2022-33/subset=warc/part-00297-d466b69e-be2b-4525-ac34-1b10d57329da.c000.gz.parquet',
 'https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2022-33/subset=warc/part-00296-d466b69e-be2b-4525-ac34-1b10d57329da.c000.gz.parquet', ... ]
```

CCFiles
--------------------


