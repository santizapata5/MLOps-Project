import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
from fastapi import FastAPI, Request, Response

pd.set_option('display.float_format', '{:.2f}'.format)

df1_ratings = pd.read_csv(r'MLOpsReviews/ratings/1.csv')
df2_ratings = pd.read_csv(r'MLOpsReviews/ratings/2.csv')
df3_ratings = pd.read_csv(r'MLOpsReviews/ratings/3.csv')
df4_ratings = pd.read_csv(r'MLOpsReviews/ratings/4.csv')
df5_ratings = pd.read_csv(r'MLOpsReviews/ratings/5.csv')
df6_ratings = pd.read_csv(r'MLOpsReviews/ratings/6.csv')
df7_ratings = pd.read_csv(r'MLOpsReviews/ratings/7.csv')
df8_ratings = pd.read_csv(r'MLOpsReviews/ratings/8.csv')

df_amazon = pd.read_csv(r'MLOpsReviews/amazon_prime_titles.csv')
df_disney = pd.read_csv(r'MLOpsReviews/disney_plus_titles.csv')
df_hulu = pd.read_csv(r'MLOpsReviews/hulu_titles.csv')
df_netflix = pd.read_csv(r'MLOpsReviews/netflix_titles.csv')

df_ratings = pd.concat([df1_ratings, df2_ratings, df3_ratings, df4_ratings, 
                        df5_ratings, df6_ratings, df7_ratings, df8_ratings])

df_ratings.isnull().sum()

df_ratings.duplicated().sum()
df_ratings.drop_duplicates(inplace=True)

df_ratings.dtypes

df_ratings['date'] = pd.to_datetime(df_ratings['timestamp'], unit='s').dt.strftime('%Y-%m-%d')

average_score = df_ratings.groupby('movieId')['rating'].mean()

df_average_score = average_score.reset_index()[['movieId', 'rating']]

print(df_amazon.duplicated().sum())
print(df_disney.duplicated().sum())
print(df_hulu.duplicated().sum())
print(df_netflix.duplicated().sum())

platforms = [df_amazon, df_disney, df_hulu, df_netflix]

df_amazon.name = 'amazon'
df_disney.name = 'disney'
df_hulu.name = 'hulu'
df_netflix.name = 'netflix'

for i in platforms:
    i['platform'] = i.name
    i.insert(loc=0, column='id', value= i.name[0]+i['show_id'])

df_platforms = pd.concat([df_amazon, df_disney, df_hulu, df_netflix])

len(df_ratings['movieId'].unique()) == len(df_platforms['id'].unique())

df_platforms['rating'] = df_platforms['rating'].fillna("G")

df_platforms['date_added'] = df_platforms['date_added'].str.strip()
df_platforms['date_added'] = pd.to_datetime(df_platforms['date_added'], format='%B %d, %Y')

df_platforms.iloc[:] = df_platforms.iloc[:].applymap(lambda x: x.lower() if isinstance(x, str) else x)

df_ratings.iloc[:] = df_ratings.iloc[:].applymap(lambda x: x.lower() if isinstance(x, str) else x)

df_platforms[['duration_int', 'duration_type']] = df_platforms['duration'].str.split(expand=True)
df_platforms['duration_int'] = df_platforms['duration_int'].fillna(0).astype(int)

app = FastAPI()

@app.get('/get_max_duration/{anio}/{plataforma}/{dtype}')

# Define the function to handle the endpoint
def get_max_duration(anio: int, plataforma: str, dtype: str):
    
    # Check if input values are valid and exist in DataFrame
    assert anio in df_platforms['release_year'].unique(), f"Invalid year: {anio}"
    assert plataforma.lower() in df_platforms['platform'].unique(), f"Invalid platform: {plataforma}"
    assert dtype.lower() in df_platforms['duration_type'].unique(), f"Invalid duration type: {dtype}"
    
    # Filter the platform data for the requested platform name, year and duration type of the movie
    filter_1 = df_platforms.loc[(df_platforms['release_year'] == anio) & 
                                (df_platforms['duration_type'] == dtype.lower()) & 
                                (df_platforms['platform'] == plataforma.lower()) & 
                                (df_platforms['type'] == 'movie')]
    
    # Check if filtered data is empty
    if filter_1.empty:
        return {"error": "No movies found with the specified criteria."}
    
    # Sort the filtered data by duration
    filter_1 = filter_1.sort_values('duration_int', ascending=False)
    
    # Find the movie(s) with the maximum duration
    max_duration = filter_1['duration_int'].max()
    
    if len(filter_1.loc[filter_1['duration_int'] == max_duration]) > 1:
        response_1 = (filter_1.loc[filter_1['duration_int'] == max_duration]).tolist()
        return {"peliculas": response_1}
    else:
        return {"pelicula": filter_1.loc[filter_1['duration_int'].idxmax(), 'title']}

df_average_score = df_average_score.rename(columns={'movieId':'id'})

df_score = pd.merge(df_platforms, df_average_score, on='id')

df_score = df_score.rename(columns={'rating_x':'rating','rating_y':'score'})

@app.get('/get_score_count/{plataforma}/{scored}/{anio}')

def get_score_count(plataforma: str, scored: float, anio: int):
    
    # Check if input values are valid and exist in DataFrame
    assert anio in df_score['release_year'].unique(), f"Invalid year: {anio}"
    assert plataforma.lower() in df_score['platform'].unique(), f"Invalid platform: {plataforma}"
    assert 0.5 <= scored <= 5.0, f"Invalid score (must be between 0.5 and 5): {scored}"
    
    # Filter the platform data for the requested platform, year and score of the movie
    filter_2 = df_score.loc[(df_score['release_year'] == anio) & 
                            (df_score['platform'] == plataforma.lower()) & 
                            (df_score['score'] > scored) &
                            (df_score['type'] == 'movie')]
    
    # Checks if filtered data is empty. If not it returns. the desired information.
    if filter_2.empty:
        return {"error": "No movies found with the specified criteria."}
    else:
        return {'plataforma': plataforma,
                'cantidad': filter_2.shape[0],
                'anio': anio,
                'score': scored}

@app.get('/get_count_platform/{plataforma}')
def get_count_platform(plataforma: str):

    # Check if input value is valid and exists in DataFrame
    assert plataforma.lower()  in df_score['platform'].unique(), f"Invalid platform: {plataforma}"
    
    # Filters the platform data for the requested platform and content type (movies only)
    filter_3 = df_score.loc[(df_score['platform'] == plataforma.lower()) &
                            (df_score['type'] == 'movie')]

    return {'plataforma': plataforma, 'peliculas': filter_3.shape[0]}

@app.get('/get_actor/{plataforma}/{anio}')
def get_actor(plataforma: str, anio: int):

    # Checks if input values are valid and exist in DataFrame
    assert plataforma.lower()  in df_score['platform'].unique(), f"Invalid platform: {plataforma}"
    assert anio in df_score['release_year'].unique(), f"Invalid year: {anio}"
    
    # Filter the data for the requested platform and year
    filter_4 = df_score.loc[(df_score['release_year'] == anio) & 
                            (df_score['platform'] == plataforma.lower())]
    
    # Checks if filtered data is empty. If not, it returns the desired information.
    if filter_4.empty:
        return {"error": "No result was found with the specified criteria."}
    else:
        # split the strings in the 'cast' column on comma separator, flatten the list of lists, remove spaces and count frequencies
        response_4 = filter_4['cast'].str.split(',').explode().str.strip().value_counts()
        return {
                'plataforma': plataforma,
                'anio': anio,
                'actor': response_4.index[0],
                'apariciones': response_4.iloc[0],
                }

@app.get('/prod_per_county/{tipo}/{pais}/{anio}')
def prod_per_county(tipo: str, pais: str, anio: int):

    # Checks if input values are valid and exist in DataFrame
    assert tipo.lower()  in df_score['type'].unique(), f"Invalid type of content: {tipo}"
    assert pais.lower()  in df_score['country'].unique(), f"Invalid country: {pais}"
    assert anio in df_score['release_year'].unique(), f"Invalid year: {anio}"
       
    # Filter the data for the requested type of content, year and country
    filter_5 = df_score.loc[(df_score['type'] == tipo.lower() ) & 
                            (df_score['country'] == pais.lower() ) & 
                            (df_score['release_year'] == anio)]       
        
    # Checks if filtered data is empty. If not, it returns the desired information.
    if filter_5.empty:
        return {"error": "No result was found with the specified criteria."}
    else:
         return {'pais': pais, 'anio': anio, 'tipo': tipo, 'peliculas': filter_5.shape[0]}

@app.get('/get_contents/{rating}')
def get_contents(rating: str):
    
    # Checks if the input value is valid and exists in DataFrame
    assert rating.lower() in df_score['rating'].unique(), f"Invalid rating: {rating}"
    
    # Filters the data for the requested audience
    filter_6 = df_score.loc[df_score['rating'] == rating.lower()]       
        
    # Checks if filtered data is empty. If not, it returns the desired information.
    if filter_6.empty:
        return {"error": "No result was found with the specified criteria."}
    else:
        return {'rating': rating, 'contenido': filter_6.shape[0]}