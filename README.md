# Movies-ETL
## Background 
The purpose of this project was to, create an automated ETL pipeline that extracts new data from multiple sources (wikipedia and Kaggle);Cleans and transforms it automatically using Pandas and regular expressions, and loads the new data into PostgreSQL.
    
For details see<[challenge.py](https://github.com/Muzznah/Movies-ETL/blob/master/Chpsycopg2allenge.py)>
  
## Resources
Data: wikipedia.movies.json, movies_metadata.csv, and ratings.csv. 

Software used: PostgreSQL 11.8, pgAdmin 4.21, Python 3.7.7, Jupytor Notebook 6.0.3, Numpy, Pandas 1.0.3, re, sqlalchemy and psycopg2.

## Assumptions and Implications

1.	The formats of the source data will remain the same; Wikipedia data will be in JSON format, and Kaggle metadata, and rating data, in 
    CSV formats. 

2. The data types and structures in each source file will remain the same so the functions put in place to format them work without a 
   fail. (Parse dollar function that converts data from strings to floats, extraction by form operations and convert timestamps to 
   datetime function)
   
3. The extraction forms defined for extracting currencies are built on dollar value and it is assumed that in the future majority of 
   budget and revenue data will continue to be a dollar amount for the function to work appropriately.  

4.	Kaggle data will always be more consistant and reliable, with minimum null values, as compared to the wikipedia data.The coding set     in place will inner join the two data sets (kaggle and wiki) to get information on movies that are present in both, and then it will     drop the redundant wiki columns(keeping unique wiki columns). Before dropping the matching columns, Wikipedia data will be used to
    fill out missing information in the revenue, budget and runtime columns.

5. The two data sets, will have important unique id columns like IMBD ids, and director(s) column, based on which movies are being 
   selected and the two data sets are being merged.

6.	While conducting the exploratory research, the column with, merged alternate_movie names, containing >90% null values, along with 
    other columns was dropped with the following code:

                wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column]\
                .isnull().sum() < len(wiki_movies_df) * 0.9]
                wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    Since we are loading data in pre-existing tables in SQL, with the specified number of columns’ and data types, we are assuming that 
    the above formula, will always filter out the same columns for ETL to work smoothly. 

7. It is assumed that any data column with more than 90% null values is not worth keeping and it will be discarded. 

8. The list of column names identified in the merge function will remain the same, for the function to catch, and merge, their values 
   within specified columns and discard the extra columns. Any new column name, that is not pre defined in the list in our function will
   not be caught and merged.

9. The users will not be interested in columns like Adult and Video (Kaggle data) that were dropped.The Video 
   column was dropped due to having "False" value, for all its rows except one.
   
10. The raw ratings data is uploaded as an additional resource for hackathon participants and it is assumed that they would not require     additional resource from the other two csvs.

