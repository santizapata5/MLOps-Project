# Import libraries
import pandas as pd
from fastapi import FastAPI, Request, Response
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Configure the float format display for 2 decimals in Pandas.
pd.set_option('display.float_format', '{:.2f}'.format)

# Extract data from csv file
df = pd.read_csv(r'data_api.csv')
df_ml = pd.read_csv(r'data_ML.csv')

# Creates an instance of the fastAPI class.
app = FastAPI()

# ---------------------------------------- Query 1 ---------------------------------------- 

@app.get('/get_max_duration/{anio}/{plataforma}/{dtype}')
def get_max_duration(anio: int, plataforma: str, dtype: str):
    
    # Check if input values are valid and exist in DataFrame
    assert anio in df['release_year'].unique(), f"Invalid year: {anio}"
    assert plataforma.lower() in df['platform'].unique(), f"Invalid platform: {plataforma}"
    assert dtype.lower() in df['duration_type'].unique(), f"Invalid duration type: {dtype}"
    
    # filtered the platform data for the requested platform name, year and duration type of the movie
    filtered = df.loc[(df['release_year'] == anio) & 
                      (df['duration_type'] == dtype.lower()) & 
                      (df['platform'] == plataforma.lower()) & 
                      (df['type'] == 'movie')]
    
    # Check if filtereded data is empty
    if filtered.empty:
        return {"error": "No movies found with the specified criteria."}
    
    # Sort the filtereded data by duration
    filtered = filtered.sort_values('duration_int', ascending=False)
    
    return {"pelicula": str(filtered.loc[filtered['duration_int'].idxmax(), 'title'])}

# ---------------------------------------- Query 2 ---------------------------------------- 

@app.get('/get_score_count/{plataforma}/{scored}/{anio}')
def get_score_count(plataforma: str, scored: float, anio: int):
    
    # Check if input values are valid and exist in DataFrame
    assert anio in df['release_year'].unique(), f"Invalid year: {anio}"
    assert plataforma.lower() in df['platform'].unique(), f"Invalid platform: {plataforma}"
    assert 0.5 <= scored <= 5.0, f"Invalid score (must be between 0.5 and 5): {scored}"
    
    # filtered the platform data for the requested platform, year and score of the movie
    filtered = df.loc[(df['release_year'] == anio) & 
                      (df['platform'] == plataforma.lower()) & 
                      (df['score'] > scored) &
                      (df['type'] == 'movie')]
    
    # Checks if filtereded data is empty. If not it returns. the desired information.
    if filtered.empty:
        return {"error": "No movies found with the specified criteria."}
    else:
        return {
            'plataforma': plataforma,
            'cantidad': int(filtered.shape[0]),
            'anio': anio,
            'score': scored
        }    

# ---------------------------------------- Query 3 ---------------------------------------- 

@app.get('/get_count_platform/{plataforma}')
def get_count_platform(plataforma: str):

    # Check if input value is valid and exists in DataFrame
    assert plataforma.lower() in df['platform'].unique(), f"Invalid platform: {plataforma}"
    
    # filtereds the platform data for the requested platform and content type (movies only)
    filtered = df.loc[(df['platform'] == plataforma.lower()) &
                      (df['type'] == 'movie')]

    return {'plataforma': plataforma, 'peliculas': int(filtered.shape[0])}

# ---------------------------------------- Query 4 ---------------------------------------- 

@app.get('/get_actor/{plataforma}/{anio}')
def get_actor(plataforma: str, anio: int):

    # Checks if input values exist in the data. Raises an error if either platform or year is not valid.
    assert plataforma.lower() in df['platform'].unique(), f"Invalid platform: {plataforma}"
    assert anio in df['release_year'].unique(), f"Invalid year: {anio}"

    # filtered the data based on the platform and year.
    filtered = df[(df['platform'] == plataforma.lower()) & 
                  (df['release_year'] == anio) &
                   df['cast'].notna()]
        
    # Checks if filtereded data is empty. If empty, returns an error. Otherwise, continues with the execution.
    if filtered.empty:
        return {"error": "No results were found in the dataset for the platform and year combination."}
    else:    
        # Extracts the 'cast' column from the filtereded dataframe, converts the values to strings, splits and expands the strings 
        # in new columns. Then, stacks these columns a in a single column, resets the index and drops the missing values.
        actors = filtered['cast'].astype(str) \
            .str.split(',', expand=True) \
            .stack() \
            .reset_index(drop=True) \
            .dropna() \
            .str.strip()
        
        # Create a Series of actors with their appearance counts.
        actors_series = actors.value_counts()  
        
        # Finds the actor with the highest number of appearances and their corresponding number of appearances.
        top_actor = actors_series.idxmax()
        appearances = actors_series[top_actor]

        # Returns the query result in a dictionary format.
        return {
            'plataforma': plataforma,
            'anio': anio,
            'actor': str(top_actor),
            'apariciones': int(appearances)
        }

# ---------------------------------------- Query 5 ---------------------------------------- 

@app.get('/prod_per_county/{tipo}/{pais}/{anio}')
def prod_per_county(tipo: str, pais: str, anio: int):

    # Checks if input values are valid and exist in DataFrame
    assert tipo.lower()  in df['type'].unique(), f"Invalid type of content: {tipo}"
    assert pais.lower()  in df['country'].unique(), f"Invalidntr couy: {pais}"
    assert anio in df['release_year'].unique(), f"Invalid year: {anio}"
       
    # filtered the data for the requested type of content, year and country
    filtered = df.loc[(df['type'] == tipo.lower()) & 
                      (df['country'] == pais.lower()) & 
                      (df['release_year'] == anio)]       
        
    # Checks if filtereded data is empty. If not, it returns the desired information.
    if filtered.empty:
        return {"error": "No result was found with the specified criteria."}
    else:
         return {'pais': pais, 'anio': anio, 'contenido': int(filtered.shape[0])}

# ---------------------------------------- Query 6 ----------------------------------------
     
@app.get('/get_contents/{rating}')
def get_contents(rating: str):
    
    # Checks if the input value is valid and exists in DataFrame
    assert rating.lower() in df['rating'].unique(), f"Invalid rating: {rating}"
    
    # filtereds the data for the requested audience
    filtered = df.loc[df['rating'] == rating.lower()]       
        
    # Checks if filtereded data is empty. If not, it returns the desired information.
    if filtered.empty:
        return {"error": "No result was found with the specified criteria."}
    else:
        return {'rating': rating, 'contenido': int(filtered.shape[0])}

# -------------------------------------- End of queries -------------------------------------- 


# ----------------------------------------- ML Query -----------------------------------------

@app.get('/get_recomendation/{title}')
def get_recomendation(title):
    
    # Extract the relevant features
    features = ['title', 'genre', 'director']
    df_features = df_ml[features].fillna('')

    # Convert the features into a list of strings
    feature_strings = df_features.apply(lambda x: ' '.join(x), axis=1)

    # Vectorize the feature strings using binary encoding
    vectorizer = CountVectorizer(binary=True)
    feature_vectors = vectorizer.fit_transform(feature_strings)

    # Compute the pairwise cosine similarity between all movies
    similarity_matrix = cosine_similarity(feature_vectors)

    # Get the top 5 similar movies for a given movie
    movie_idx = df_ml[df_ml['title'] == title].index[0]
    similarities = list(enumerate(similarity_matrix[movie_idx]))
    similarities = sorted(similarities, key=lambda x: x[1], reverse=True)

    return {'recomendacion':str([df_ml.iloc[idx]['title'] for idx, _ in similarities[1:6]])}