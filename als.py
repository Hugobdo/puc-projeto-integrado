from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

spark = SparkSession\
        .builder\
        .appName("ALSExample")\
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/puc2.recomendacoes") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/puc2.recomendacoes") \
        .config('spark.jars.packages',"org.mongodb.spark:mongo-spark-connector_2.12:10.1.1")\
        .getOrCreate()

lines = spark.read.text("sample_movielens_ratings.txt").rdd
parts = lines.map(lambda row: row.value.split("::"))
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
                                     rating=float(p[2]), timestamp=int(p[3])))
ratings = spark.createDataFrame(ratingsRDD.collect())

(training, test) = ratings.randomSplit([0.8, 0.2])

als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
              coldStartStrategy="drop")
model = als.fit(training)

predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

userRecs = model.recommendForAllUsers(10)

users = ratings.select(als.getUserCol()).distinct()

userRecsOnlyItemId = userRecs.select(userRecs['userId'], userRecs['recommendations']['movieId'])

userRecs.select(userRecs["userId"], \
                userRecs["recommendations"]["movieId"].alias("movieId"),\
                userRecs["recommendations"]["rating"].cast('array<double>').alias("rating"))\
    .write\
    .format("com.mongodb.spark.sql.connector.MongoTableProvider")\
    .option("database", "puc2")\
    .option("collection", "recomendacoes")\
    .mode("append")\
    .save()