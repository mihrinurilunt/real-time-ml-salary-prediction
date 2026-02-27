import joblib
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# Spark Streaming ile Kafka'dan Veri Alma ve Modelle Tahmin Yapma
def start_spark_streaming():
    # Step 1: SparkSession oluştur
    spark = SparkSession.builder \
        .appName("Kafka-Spark-ML-Streaming") \
        .config("spark.master", "local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

    # Step 2: Kafka'dan veri oku
    KAFKA_TOPIC = "salary-topic"
    KAFKA_BROKER = "localhost:9092"
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Kafka'dan gelen mesajları string'e çevir
    df = df.selectExpr("CAST(value AS STRING)")

    # Step 3: Veriyi ayıklama
    columns = df.selectExpr("split(value, ',') as data") \
        .selectExpr("data[0] as Position", "CAST(data[1] AS INT) as Level", "CAST(data[2] AS DOUBLE) as Salary")

    # Step 4: Model ve encoder'ı yükle
    model_path = '/home/elif/Desktop/DataAnalysis/ml/trained_model.joblib'
    encoder_path = '/home/elif/Desktop/DataAnalysis/ml/encoder.joblib'
    model = joblib.load(model_path)
    encoder = joblib.load(encoder_path)

    # Step 5: Feature oluştur
    assembler = VectorAssembler(inputCols=["Level"], outputCol="features")
    data = assembler.transform(columns)

    # Step 6: Tahmin yapmak için UDF (User Defined Function) oluştur
    def predict_salary(level, position):
        encoded_position = encoder.transform([position])[0]  # Position'ı encode et
        return float(model.predict([[encoded_position, level]]))  # Tahmin yap

    predict_udf = udf(predict_salary, DoubleType())

    # Step 7: Tahminleri veri çerçevesine ekle
    predictions = data.withColumn("prediction", predict_udf(data["Level"], data["Position"]))

    # Step 8: Sonuçları Kafka'ya veya konsola yazdır
    query = predictions.select("Position", "Level", "Salary", "prediction") \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch_id: df.show()) \
        .start()

    # Step 9: Streaming işlemini başlat
    query.awaitTermination()

if __name__ == "__main__":
    start_spark_streaming()
