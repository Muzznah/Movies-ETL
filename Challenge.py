#Import dependencies.
import json
import pandas as pd
import numpy as np
import re
import psycopg2
from sqlalchemy import create_engine
from config import db_password
import time

#Files Path.
file_dir='C:/Users/muzzn/Class/Data-Movies-ETL/'

#load files.
kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
ratings = pd.read_csv(f'{file_dir}ratings.csv')
with open(f'{file_dir}/wikipedia.movies.json', mode='r') as file:
    wiki_movies= json.load(file)
    
    def ETL_Auto(wiki_movies, kaggle_metadata, ratings):
 
     
    
    #___________________Transformation Steps for wiki data______________________________.
    
    
        #should not be a serial and should have IMDB number and a director.
        wiki_movies = [movie for movie in wiki_movies
                if ('Director' in movie or 'Directed by' in movie) 
                    and 'imdb_link' in movie
                    and 'No. of episodes' not in movie]
        
    
            #-----function: merges column names-----
        
        def clean_movie(movie):
            movie = dict(movie) #create a non-destructive copy
            alt_titles = {}
            # combine alternate titles into one list
            for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                        'Hangul','Hebrew','Hepburn','Japanese','Literally',
                        'Mandarin','McCune-Reischauer','Original title','Polish',
                        'Revised Romanization','Romanized','Russian',
                        'Simplified','Traditional','Yiddish']:
                if key in movie:
                    alt_titles[key] = movie[key]
                    movie.pop(key)
            if len(alt_titles) > 0:
                movie['alt_titles'] = alt_titles
        
            def change_column_name(old_name, new_name):
                if old_name in movie:
                    movie[new_name] = movie.pop(old_name)
                    
            change_column_name('Adaptation by', 'Writer(s)')
            change_column_name('Country of origin', 'Country')
            change_column_name('Directed by', 'Director')
            change_column_name('Distributed by', 'Distributor')
            change_column_name('Edited by', 'Editor(s)')
            change_column_name('Length', 'Running time')
            change_column_name('Original release', 'Release date')
            change_column_name('Music by', 'Composer(s)')
            change_column_name('Produced by', 'Producer(s)')
            change_column_name('Producer', 'Producer(s)')
            change_column_name('Productioncompanies ', 'Production company(s)')
            change_column_name('Productioncompany ', 'Production company(s)')
            change_column_name('Released', 'Release Date')
            change_column_name('Release Date', 'Release date')
            change_column_name('Screen story by', 'Writer(s)')
            change_column_name('Screenplay by', 'Writer(s)')
            change_column_name('Story by', 'Writer(s)')
            change_column_name('Theme music composer', 'Composer(s)')
            change_column_name('Written by', 'Writer(s)')    
        
            return movie

        #Convert to dataframe.
        clean_movies = [clean_movie(movie) for movie in wiki_movies]
        wiki_movies_df = pd.DataFrame(clean_movies)
        
        #Extract imdb ids from links.
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        
        #drop duplicates.
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
        
        #Keep columns with <90% null values only.
        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
        wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
        
        #Make a data series for box office, budget, release date and running time, drop na values, and convert lists to strings.
        box_office = wiki_movies_df['Box office'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        budget = wiki_movies_df['Budget'].dropna().map(lambda x: ' '.join(x) if type(x) == list else x)
        release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        
        #Convert ranges to $ ammounts.
        R = r'\$.*[-—–](?![a-z])'
        box_office = box_office.str.replace(R, '$', regex=True)
        budget = budget.str.replace(R, '$', regex=True)
        
        #Remove citation.
        budget = budget.str.replace(r'\[\d+\]\s*', '')
        
        #Forms to extract.
        form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
        
        

        # -----function: converts strings to floating points-----
        
        def parse_dollars(s):
            # if s is not a string, return NaN
            if type(s) != str:
                return np.nan
            
            # if input is of the form $###.# million
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
                # remove dollar sign and " million"
                s = re.sub('\$|\s|[a-zA-Z]','', s)
                # convert to float and multiply by a million
                value = float(s) * 10**6
                # return value
                return value

            # if input is of the form $###.# billion
            elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
                # remove dollar sign and " billion"
                s = re.sub('\$|\s|[a-zA-Z]','', s)
                # convert to float and multiply by a billion
                value = float(s) * 10**9
                # return value
                return value

            # if input is of the form $###,###,###
            elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
                # remove dollar sign and commas
                s = re.sub('\$|,','', s)
                # convert to float
                value = float(s)
                # return value
                return value

            # otherwise, return NaN
            else:
                return np.nan
        
        #Extract form 1 &2 and apply parse dollar function to box office and budget.
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        
        # Date forms to be extracted from Release date column.
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'
        
        #Extract forms 1-4 and apply pandas to_datetime() method to release dates.Since there are different date formats, set the infer_datetime_format option to True
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
        
        #Extract digits from both forms.
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
        #Convert string to numeric and coerce errors to zero.
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        #Convert all capture groups to minutes.
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        
        #Drop redundant columns
        wiki_movies_df.drop(['Box office','Budget','Running time','Release date'] , axis=1, inplace=True)
        
        #___________________Transformation Steps for kaggle data______________________________.
        
        
        #keep data where adult is false and then drop the adult column.also drop video as it doesnt offer much info.
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop(['adult','video'],axis='columns')
        
        # For the numeric columns,use the to_numeric() method.
        # Set errors argument to 'raise', to identify data that can’t be converted to numbers.
        
        try:
            kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        except:
            print("Could not be converted to an integer datatype....Skipping...")
            
        try:
            kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        except:
            print("Could not be converted to a numeric datatype....Skipping...")
            
        try:
            kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
        except:
            print("Could not be converted to a numeric datatype....Skipping...")
            
        try:
        #Convert release_date to datetime using;to_datetime().
            kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
        except:
            print("Could not convert date to datetime....Skipping...")
        
        
        #___________________Merging data sets______________________________.
        
        #inner join to keep only movies present in both.suffixes will help identify data.
        movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
        
        # The Holiday in the Wikipedia data got merged with From Here to Eternity, droping that using index number.
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
        
        #Kaggle data being more consistant and reliable we will drop the wiki data and use it only to fill missing values in kaggle.
        
        # First, we’ll drop the title_wiki, release_date_wiki, Language, and Production company(s) columns.
        movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
        
        # Next, to save a little time, we’ll make a function that fills in missing data for a column pair and then drops the redundant column.
        def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        
            df.drop(columns=wiki_column, inplace=True)
            
        # Now we can run the function for the three column pairs that we decided to fill in zeros.
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
        
        #Reorder columns.
        movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                        'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                        'genres','original_language','overview','spoken_languages','Country',
                        'production_companies','production_countries','Distributor',
                        'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                        ]]
        
        #Rename columns.
        movies_df.rename({'id':'kaggle_id',
                    'title_kaggle':'title',
                    'url':'wikipedia_url',
                    'budget_kaggle':'budget',
                    'release_date_kaggle':'release_date',
                    'Country':'country',
                    'Distributor':'distributor',
                    'Producer(s)':'producers',
                    'Director':'director',
                    'Starring':'starring',
                    'Cinematography':'cinematography',
                    'Editor(s)':'editors',
                    'Writer(s)':'writers',
                    'Composer(s)':'composers',
                    'Based on':'based_on'
                    }, axis='columns', inplace=True)
        
        #___________________Transformation and merging ratings data______________________________.
        
        try:
            # Convert timestamp column to datetime.
            ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
        except:
            print("Could not convert timestamp to datetime....Skipping...")
        
        # Groupby movieid and ratings to get a count, rename userid to count, and convert df to pivot table.
        rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating', values='count')
        
        # Add suffix to column names.
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
        
        
        # Left join with movies_df, to keep ratings for only the movies that are available in movies_df.
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
        
        # Replace any null-values for ratings with zero.
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
        
        
        #___________________Load files to Postgress ______________________________.
        
        # Create engine, the connection string will be as follows:
        db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
        engine = create_engine(db_string)
        
        try:
            #Import the movies data into pre-existing table.
            movies_df.to_sql(name='movies', con=engine,if_exists='append')
        except:
            print("Difficulty in loading movies data....Skipping...")
        
        try:
            #import the raw ratings data into pre_existing table.
            rows_imported = 0
            # get the start_time from time.time()
            start_time = time.time()
            for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):

                print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')

                data.to_sql(name='ratings', con=engine, if_exists='append')
                rows_imported += len(data)

                # add elapsed time to final print out
                print(f'Done. {time.time() - start_time} total seconds elapsed')
        except:
            print("Could not load ratings data")
        
    #Call ETL_Auto function.    
    ETL_Auto(wiki_movies, kaggle_metadata, ratings)

