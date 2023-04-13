# Import libraries
import pandas as pd
from fastapi import FastAPI, Request, Response

# Configure the float format display for 2 decimals in Pandas.
pd.set_option('display.float_format', '{:.2f}'.format)

# Extract data from csv file
df_score = pd.read_csv(r'data.csv')

# Creates an instance of the fastAPI class.
app = FastAPI()

# ---------------------------------------- Query 1 ---------------------------------------- 

@app.get('/get_max_duration/{anio}/{plataforma}/{dtype}')
def get_max_duration(anio: int, plataforma: str, dtype: str):
    
    # Check if input values are valid and exist in DataFrame
    assert anio in df_score['release_year'].unique(), f"Invalid year: {anio}"
    assert plataforma.lower() in df_score['platform'].unique(), f"Invalid platform: {plataforma}"
    assert dtype.lower() in df_score['duration_type'].unique(), f"Invalid duration type: {dtype}"
    
    # Filter the platform data for the requested platform name, year and duration type of the movie
    filter_1 = df_score.loc[(df_score['release_year'] == anio) & 
                                (df_score['duration_type'] == dtype.lower()) & 
                                (df_score['platform'] == plataforma.lower()) & 
                                (df_score['type'] == 'movie')]
    
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

# ---------------------------------------- Query 2 ---------------------------------------- 

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

# ---------------------------------------- Query 3 ---------------------------------------- 

@app.get('/get_count_platform/{plataforma}')
def get_count_platform(plataforma: str):

    # Check if input value is valid and exists in DataFrame
    assert plataforma.lower()  in df_score['platform'].unique(), f"Invalid platform: {plataforma}"
    
    # Filters the platform data for the requested platform and content type (movies only)
    filter_3 = df_score.loc[(df_score['platform'] == plataforma.lower()) &
                            (df_score['type'] == 'movie')]

    return {'plataforma': plataforma, 'peliculas': filter_3.shape[0]}

# ---------------------------------------- Query 4 ---------------------------------------- 

@app.get('/get_actor/{plataforma}/{anio}')
def get_actor(plataforma: str, anio: int):

    # Checks if input values exist in the data. Raises an error if either platform or year is not valid.
    assert plataforma.lower() in df_score['platform'].unique(), f"Invalid platform: {plataforma}"
    assert anio in df_score['release_year'].unique(), f"Invalid year: {anio}"

    # Filter the data based on the platform and year.
    filter_4 = df_score[df_score['platform'] == plataforma.lower() & 
                        df_score['release_year'] == anio &
                        df_score['cast'].notna()]
    
    filter_4 = df_score[(df_score['platform'] == plataforma.lower()) & 
                        (df_score['release_year'] == anio) &
                         df_score['cast'].notna()]
        
    # Checks if filtered data is empty. If empty, returns an error. Otherwise, continues with the execution.
    if filter_4.empty:
        return {"error": "No results were found in the dataset for the platform and year combination."}
    else:    
        # Extracts the 'cast' column from the filtered dataframe, converts the values to strings, splits and expands the strings 
        # in new columns. Then, stacks these columns a in a single column, resets the index and drops the missing values.
        actors = filter_4['cast'].astype(str).str.split(',', expand=True).stack().reset_index(drop=True).dropna()
        
        # Create a Series of actors with their appearance counts.
        actors_series = actors.value_counts()  
        
        # Finds the actor with the highest number of appearances and their corresponding number of appearances.
        top_actor = actors_series.idxmax()
        appearances = actors_series[top_actor]

        # Returns the query result in a dictionary format.
        return {'plataforma': plataforma,
                'anio': anio,
                'actor': top_actor,
                'apariciones': int(appearances)}

# ---------------------------------------- Query 5 ---------------------------------------- 

@app.get('/prod_per_county/{tipo}/{pais}/{anio}')
def prod_per_county(tipo: str, pais: str, anio: int):

    # Checks if input values are valid and exist in DataFrame
    assert tipo.lower()  in df_score['type'].unique(), f"Invalid type of content: {tipo}"
    assert pais.lower()  in df_score['country'].unique(), f"Invalidntr couy: {pais}"
    assert anio in df_score['release_year'].unique(), f"Invalid year: {anio}"
       
    # Filter the data for the requested type of content, year and country
    filter_5 = df_score.loc[(df_score['type'] == tipo.lower() ) & 
                            (df_score['country'] == pais.lower() ) & 
                            (df_score['release_year'] == anio)]       
        
    # Checks if filtered data is empty. If not, it returns the desired information.
    if filter_5.empty:
        return {"error": "No result was found with the specified criteria."}
    else:
         return {'pais': pais, 'anio': anio, 'peliculas': filter_5.shape[0]}

# ---------------------------------------- Query 6 ----------------------------------------
     
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
    
        # ANTES del error
        return {'plataforma': plataforma,
                'anio': anio,
                'actor': actor_counts.iloc[0,0],
                'apariciones': actor_counts.iloc[0,1]}
        
        # DESPUÉS de corregir
        return {'plataforma': plataforma,
                'anio': anio,
                'actor': actor_counts.iloc[0,0],
                'apariciones': int(actor_counts.iloc[0,1])}

# -------------------------------------- End of queries -------------------------------------- 