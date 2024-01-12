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


| Syntax      | Description |
| ----------- | ----------- |
| Header      | Title       |
| Paragraph   | Text        |





The script for ColumnarExplorer extracts files in columnar format for a user defined monh and year. It is currently self contained and can be found in [`columnar_explorer.py`](https://github.com/giuliaok/crawls_nest/blob/main/scripts/columnar_explorer.py). 

The class has got 6 main attributes: 

- *schema*: CommonCrawl  

CCFiles
--------------------


