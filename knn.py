import pandas as pd
import pymongo
import numpy as np
from scipy.spatial.distance import hamming


nome_arq = 'ratings.csv'
dados = pd.read_csv(nome_arq,sep=",", header=0, names=["userId", "movieId", "rating", "timestamp"])

filme_arq = 'movies.csv'
filmes = pd.read_csv(filme_arq, sep=",", header=0, names=["movieId", "titulo", "genero"])

def filme_meta(id_filme):
    titulo = filmes[filmes['movieId']==id_filme]['titulo'].values[0]
    genero = filmes[filmes['movieId']==id_filme]['genero'].values[0]
    return titulo, genero

def filme_meta_titulo(id_filme):
    titulo = filmes[filmes['movieId']==id_filme]['titulo'].values[0]
    return titulo

def filme_meta_genero(id_filme):
    genero = filmes[filmes['movieId']==id_filme]['genero'].values[0]
    return genero

def filmes_favoritos(usuario, N):
    ratings_usuario = dados[dados['userId']==usuario]
    rating_usuario_ordenado = pd.DataFrame.sort_values(ratings_usuario,['rating'],ascending=[0])[:N]
    rating_usuario_ordenado['titulo'] = rating_usuario_ordenado['movieId'].apply(filme_meta_titulo)
    rating_usuario_ordenado['genero'] = rating_usuario_ordenado['movieId'].apply(filme_meta_genero)
    return rating_usuario_ordenado

def distancia(usuario1, usuario2):
    try:
        ratings_usuario_1 = matriz_ratings.transpose()[usuario1]
        ratings_usuario_2 = matriz_ratings.transpose()[usuario2]
        distancia = hamming(ratings_usuario_1, ratings_usuario_2)
    except:
        distancia = np.NaN
        
    return distancia

def vizinhos_mais_proximos(usuario, K=10):
    todos_usuarios = pd.DataFrame(matriz_ratings.index)
    todos_usuarios = todos_usuarios[todos_usuarios.userId != usuario]
    todos_usuarios['distancia'] = todos_usuarios['userId'].apply(lambda x: distancia(usuario, x))
    K_vizinhos_usuario = todos_usuarios.sort_values(['distancia'], ascending=True)['userId'][:10]
    
    return K_vizinhos_usuario

def top_filmes(usuario, N=3):
    K_vizinhos = vizinhos_mais_proximos(usuario, 5)
    ratings_vizinhos = matriz_ratings[matriz_ratings.index.isin(K_vizinhos)]
    rating_medio = ratings_vizinhos.apply(np.nanmean).dropna()
    filmes_vistos_pelo_usuario = matriz_ratings.transpose()[usuario].dropna().index
    rating_predito = rating_medio[~rating_medio.index.isin(filmes_vistos_pelo_usuario)]
    rating_predito = rating_predito.sort_values(ascending=False)
    top_filmes = rating_predito.sort_values(ascending=False).index[:N]
    
    recomendacao = pd.DataFrame(top_filmes)
    recomendacao['titulo'] = recomendacao.movieId.apply(filme_meta)
    recomendacao['pred'] = rating_predito.values[:N]
    recomendacao['usuario'] = usuario
    return recomendacao

dados = dados[dados['movieId'].isin(filmes['movieId'])]

dados = dados[dados.movieId.isin(usuarios_por_filme[usuarios_por_filme > 10].index)]

dados = dados[dados.userId.isin(filmes_por_usuario[filmes_por_usuario > 10].index)]

matriz_ratings = pd.pivot_table(dados, values='rating', index=['userId'], columns=['movieId'])

resultado = pd.DataFrame()
for usuario in list(lista_usuarios)[:10]:
    recomendacao = top_filmes(usuario, 3)
    resultado = pd.concat([recomendacao, resultado])

cliente = pymongo.MongoClient("mongodb://localhost:27017")
base = cliente["recomendacao"]
colecao = base["recomendacoes_mongo"]
colecao.insert_many(resultado.to_dict('records'))



