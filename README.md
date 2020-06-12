# Movies-ETL
## Background 
The purpose of this project was to, create an automated ETL pipeline that:

    -Extracts new data from multiple sources (wikipedia and Kaggle)

    -Cleans and transforms it automatically using Pandas and regular expressions.

    -And loads the new data into PostgreSQL.
    
For details see<[challenge.py](https://github.com/Muzznah/Movies-ETL/blob/master/Challenge.py)>
  
## Resources
Software used: PostgreSQL 11.8 and pgAdmin 4.21
## Assumptions and Implications

1.	The formats of the source data will remain the same; Wikipedia data will be in JSON format, and Kaggle metadata, and rating data, in 
    CSV formats. 

2. The data types and structures in each source file will remain the same so the functions put in place to format them work without a 
   fail.

3.	The data will have important columns like IMBD ids and director column based on which movies are being selected.

4.	While conducting the exploratory research, the column with merged, alternate movie names, resulted in >90% null values and was 
    dropped with the following code:

                wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column]\
                .isnull().sum() < len(wiki_movies_df) * 0.9]
                wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

   Since we are loading data in pre-existing tables in SQL, with the specified number of columns’ and data types, we are assuming that 
   the above formula, will always filter out the same columns for ETL to work smoothly. 

5. Any data column with more than 90% null values is not worth keeping and it will be discarded. 

6. The list of column names identified in the merge function will remain the same, for the function to catch, and merge, their values 
   within specified columns and discard the extra columns. Any new column name that is not pre defined in the list in our function will
   not be caught and merged.

7. The users will not be interested in columns like Adult and Video (Kaggle data) that were dropped.

