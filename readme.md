The data used can be found here : https://dumps.wikimedia.org/other/pageview_complete/

The preprocessing of the data to create a clean dataset has been done using a remote cluster at SFU to leverage parallelism through pyspark.

The objective is to build a time series model to predict page views
