import pandas as pd
import time
import joblib
import os
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Kafka Üreticisi (Producer)
def send_to_kafka():
    try:
        # CSV dosya yolu ve Kafka parametreleri
        csv_file = '/home/elif/Desktop/DataAnalysis/data/salary.csv'
        KAFKA_TOPIC = 'salary-topic'
        KAFKA_BROKER = 'localhost:9092'

        # Kafka Producer oluştur
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

        # CSV dosyasını oku
        df = pd.read_csv(csv_file)

        # Her satırı Kafka'ya gönder
        for index, row in df.iterrows():
            message = f"{row['Position']},{row['Level']},{row['Salary']}"
            # Mesajı Kafka'ya gönder
            future = producer.send(KAFKA_TOPIC, message.encode('utf-8'))
            # Mesajın başarılı bir şekilde gönderildiğini kontrol et
            result = future.get(timeout=10)
            print(f"Sent: {message} to {result.topic}")

            time.sleep(1)  # Mesajlar arası gecikme (isteğe bağlı)

        # Kafka Producer'ı kapat
        producer.flush()  # Mesajların tümünün gönderildiğinden emin ol
        producer.close()

        print("Tüm veriler gönderildi!")

    except KafkaError as e:
        print(f"Kafka hatası: {e}")
    except Exception as e:
        print(f"Genel hata oluştu: {e}")


# Makine Öğrenimi Modeli Eğitimi ve Kaydetme
def train_and_save_model():
    try:
        # Step 1: Load the dataset
        data = pd.read_csv('/home/elif/Desktop/DataAnalysis/data/salary.csv')

        # Step 2: Preprocess the data
        encoder = LabelEncoder()
        data['Position'] = encoder.fit_transform(data['Position'])

        # Features (X) and target (y)
        X = data[['Position', 'Level']]
        y = data['Salary']

        # Step 3: Split the data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Step 4: Train the machine learning model (Random Forest Regressor)
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)

        # Step 5: Evaluate the model
        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        print(f"Mean Absolute Error: {mae}")

        # Step 6: Save the trained model to a file
        model_path = '/home/elif/Desktop/DataAnalysis/ml/trained_model.joblib'
        encoder_path = '/home/elif/Desktop/DataAnalysis/ml/encoder.joblib'

        # Ensure the directory exists
        os.makedirs('/home/elif/Desktop/DataAnalysis/ml', exist_ok=True)

        # Save the model and encoder
        joblib.dump(model, model_path)
        joblib.dump(encoder, encoder_path)

        print("Model and encoder trained and saved.")

    except Exception as e:
        print(f"Model training and saving error: {e}")


if __name__ == '__main__':
    # Kafka'ya veri gönder
    send_to_kafka()

    # Modeli eğit ve kaydet
    train_and_save_model()
