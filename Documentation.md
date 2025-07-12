First the dataset from kaggle had two folders , one contained RAW JSON files , and other contained RAW CSV files , both are linked by id and category id . 

Used command prompt to upload all the files and the CSV files were uploaded partitioning according to their regions , created IAM roles for Glue , S3 and Lamda , giving access to s3 and other permissions , then the RAW JSON files had nested array inside it , which had to be converted to parquet format , hence instead of GLUE crawler , lamda was used to convert the following json to parquet , 

Then GLue crawler was used to get the table schema for each , later this same lamda function was added with an S3 trigger to automate the processing of JSON files , that is whenever a new json file was uploaded , the files were automatically stores in a cleaned s3 bucket which had the cleaned data 

The CSV data was converted using ETL visual job in GLUE , this helped to convert all the csv files to parquet partitioning according to its region , then , after the cleaned bucket was ready , glue crawler was used to get the table schema of the cleaned csv files 

ETL visual job in GLUE was used again to merge all the parquet files , that is from both the json and csv data , a new storage , database and table was created to store the merged data for a simpler,cleaner and efficient way for analysis . 

Then this database , was  imported to AWS quicksight for FINAL DATA ANALYSIS OF YOUTUBE data 

various types of charts and graphs were used to compare the trend and check the analysis of data.
