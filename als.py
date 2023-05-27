from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

def create_spark_session():
    """
    Cria uma sessão do Spark.

    Returns:
        Uma instância de `SparkSession`.
    """
    return SparkSession.builder \
        .appName("ALSExample") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/puc2.recomendacoes") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/puc2.recomendacoes") \
        .config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .getOrCreate()

def load_ratings_data(spark):
    """
    Carrega os dados de classificação de avaliações em um DataFrame.

    Args:
        spark: A sessão do Spark.

    Returns:
        Um DataFrame contendo as avaliações.
    """
    lines = spark.read.text("sample_movielens_ratings.txt").rdd
    parts = lines.map(lambda row: row.value.split("::"))
    ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
                                         rating=float(p[2]), timestamp=int(p[3])))
    ratings = spark.createDataFrame(ratingsRDD.collect())
    return ratings

def split_train_test_data(ratings):
    """
    Divide os dados de avaliações em conjuntos de treinamento e teste.

    Args:
        ratings: O DataFrame contendo as avaliações.

    Returns:
        Uma tupla contendo os conjuntos de treinamento e teste.
    """
    return ratings.randomSplit([0.8, 0.2])

def train_als_model(training_data):
    """
    Treina um modelo ALS (Alternating Least Squares) usando os dados de treinamento.

    Args:
        training_data: O conjunto de dados de treinamento.

    Returns:
        O modelo treinado.
    """
    als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
              coldStartStrategy="drop")
    model = als.fit(training_data)
    return model

def evaluate_model(model, test_data):
    """
    Avalia o modelo usando os dados de teste e imprime o erro quadrático médio.

    Args:
        model: O modelo treinado.
        test_data: O conjunto de dados de teste.
    """
    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))

def get_user_recommendations(model):
    """
    Gera recomendações para todos os usuários com base no modelo treinado.

    Args:
        model: O modelo treinado.

    Returns:
        Um DataFrame contendo as recomendações para todos os usuários.
    """
    userRecs = model.recommendForAllUsers(10)
    return userRecs

def save_user_recommendations(userRecs):
    """
    Salva as recomendações de usuário em um banco de dados MongoDB.

    Args:
        userRecs: O DataFrame contendo as recomendações de usuário.
    """
    users = userRecs.select(userRecs['userId']).distinct()
    userRecsOnlyItemId = userRecs.select(userRecs['userId'], userRecs['recommendations']['movieId'])

    userRecs.select(userRecs["userId"], \
                    userRecs["recommendations"]["movieId"].alias("movieId"), \
                    userRecs["recommendations"]["rating"].cast('array<double>').alias("rating")) \
        .write \
        .format("com.mongodb.spark.sql.connector.MongoTableProvider") \
        .option("database", "puc2") \
        .option("collection", "recomendacoes") \
        .mode("append") \
        .save()

spark = create_spark_session()
ratings = load_ratings_data(spark)
training, test = split_train_test_data(ratings)
model = train_als_model(training)
evaluate_model(model, test)
userRecs = get_user_recommendations(model)
save_user_recommendations(userRecs)
