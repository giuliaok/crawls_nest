# crawls_nest

Repository hosting code to access the Common Crawl from your own machine 🥳

...work in progress

**NB**: This Readme file is intended for users! If you would like to contribute to this repo, please refer to the [`guide for contributors`](https://github.com/giuliaok/crawls_nest/blob/main/guide_for_contributors.md)

Background
----------

The code builds on the [`warcio`](https://github.com/webrecorder/warcio) library for fast [WARC
Format](<https://en.wikipedia.org/wiki/Web_ARChive>) reading and writing. 

Fetching records from Common Crawl
---------

crawls_nest depends on the
[`webrecorder
/
warcio`](https://github.com/webrecorder/warcio)
library to read records and content of WARC files. Please see the
[`warcio documentation`](https://github.com/webrecorder/warcio/blob/master/README.rst) for
usage instructions of the library.

Crawl's nest usage
---------

### File collection

The ```columnar_explorer.py``` script requires two arguments: ```month``` and ```year```, which serve to define the chunks of the crawl we want to work with. We provide two further optional arguments:
```domain``` defines a specific domain to extract from the crawl (e.g. *.com*, *.org*). If the domain is unspecified, the script will extract all websites from the crawl. ```clean``` subsets the columnar index by removing some often empty features of the crawl. The argument is True by default. 
Below is an example of how to use the script from the command line:
```shell script
$ python columnar_explorer.py --month august --year 2022 --domain org
```

### File processing 

The ```process_warc_files.py``` script requires one argument: ```data_directory```, which is a path to the dataset containing the files extracted in columar format through ```columnar_explorer.py``` (or whichever other script you may want to use). We provide five further optional arguments: ```get_html```, which is False by default, to extract html hyperlinks from **wat** records. ```get```, which is used to extract and process text from **wet** records, ```geography``` which defines the geography of choice when searching postcodes within a record's text, if left empty all postcodes are extracted, ```classification``` which defines whether we want to provide an initial industrial classification for each website and finally ```saving_freq```. This last argument is quite important as it defines after how many processed records to save a copy of the results: these datasets tends to be incredibly big and it is advisable to work in batches. 
Below is an example of how to use the script from the command line: 
```shell script
$ python process_warc_files.py --data_directory data_directory --get True --geography London --classification False --saving_freq 
```

### Requirements

This project is based on ```python==3.10.8``` but should work on previous versions as well after 3.7. The dependencies are as follow:
```
transformers==4.36.0
requests==2.28.2
gensim==3.8.1
warcio==1.7.4
numpy==1.24.1
beautifulsoup4==4.12.0
```
