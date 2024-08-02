Here're the descriptions of each folder:

1. ana_code:
	- contains a simple data analysis of genres.csv and pyspark code (Pyspark_Based_Genre_Classification.ipynb) which utilizes machine learning library for music genre classification task based on features data extracted from echonest_clean.csv and features_clean.csv.
	- the pyspark code is written on Google colab using spark platform, thus it is most convenient to implement the code using colab. The folder which includes all necessary datasets and the code can be found on Google drive [here](https://drive.google.com/drive/folders/1ibg-kWsWLmZw6bgbOZ6Iv5URCyaIz9CV?usp=sharing).
	- the output can be easily found in the jupyter notebook. It is worth mentioning that some cross-validation cells take much time to run; thus, it is recommended to only run the cells that trains models directly with a single set of hyper-parameters.
	- also contains data analysis of tracks.csv studying the relationship between artist and track genre, and potential geographical effects on the track genre.

2. data_ingest:
	- contains simple data ingestion of all four datasets. Each scala code loads corresponding dataset to spark on dataproc and show the first five rows and column names.
	

3. etl_code:
	- contains all cleaning scala files for the four datasets: genres.csv, tracks.csv, echonest.csv, and features.csv.
	- the output files can be found in corresponding cleaned csv documents. E.g. the cleaned version of echonest.csv is echonest_clean.csv.
	- however, if one runs the scala code directly using spark on dataproc, it will genreate csv files with system-default names in corresponding folders, e.g. part-00000-d17b1021-e1b4-4ab3-9d5c-7dea3f1e56ae-c000.csv in echonest_clean, which has same contents with echonest_clean.csv

4. profiling_code:
	- contains profiling code for echonest.csv and genres.csv.
	- the output can be viewed using spark on dataproc. Notice that the results are stored in scala values; thus, one might need to access each variable to get the results, or simply pasting each line of code into spark interface on dataproc to see the results.

5. Data:
	- contains all the input data and cleaned data we used.