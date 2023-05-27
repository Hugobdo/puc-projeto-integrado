from fastapi import FastAPI
from pymongo import MongoClient

app = FastAPI()

# Configuração da conexão com o MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["puc2"]
collection = db["recomendacoes"]

@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: int):
    """
    Obtém as recomendações para um usuário específico.

    Args:
        user_id: O ID do usuário.

    Returns:
        Um dicionário JSON contendo o ID do usuário e suas recomendações correspondentes, se encontradas.
        Caso contrário, retorna uma mensagem de erro indicando que o usuário não foi encontrado.
    """
    # Consulta o banco de dados para obter as recomendações do usuário específico
    result = collection.find_one({"userId": user_id})

    if result:
        recommendations = list(zip(result["movieId"], result["rating"]))
        return {"user_id": user_id, "recommendations": recommendations}
    else:
        return {"message": "User not found"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
