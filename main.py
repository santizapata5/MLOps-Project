import pandas as pd
from datetime import datetime
from fastapi import FastAPI, Request, Response

pd.set_option('display.float_format', '{:.2f}'.format)

df_score= pd.read_csv(r'data.csv')

app = FastAPI()

@app.get('/get_max_duration/{anio}/{plataforma}/{dtype}')

# Define the function to handle the endpoint
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
    assert plataforma.lower() in df_score['platform'].unique(), f"Invalid platform: {plataforma}"
    assert int(anio) in df_score['release_year'].unique(), f"Invalid year: {anio}"
    anio = int(anio)
    
    # Filter the data for the requested platform and year
    filter_4 = df_score.loc[(df_score['release_year'] == anio) & 
                            (df_score['platform'] == plataforma.lower())]
    
    # Checks if filtered data is empty. If not, it returns the desired information.
    if filter_4.empty:
        return {"error": "No result was found with the specified criteria."}
    else:
        df_cast = filter_4.assign(actor = df_score['cast'].str.split(',')).explode('actor')
        response_4 = df_cast['actor'].value_counts()

        return {
                'plataforma': plataforma,
                'anio': anio,
                'actor': response_4.index[0],
                'apariciones': response_4.iloc[0]
                }


    


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