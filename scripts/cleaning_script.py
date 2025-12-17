import pandas as pd
import s3fs
from sqlalchemy import create_engine
import os

# --- AYARLAR ---
# Bu ayarları env variable'dan almak best practice'dir ama ödev için hardcode edebiliriz
# veya docker-compose'da environment altına ekleyebilirsin.
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin" # Kendi şifren
MINIO_SECRET_KEY = "minioadmin" # Kendi şifren
POSTGRES_CONN = "postgresql://airflow:airflow@postgres/traindb" # user:pass@host/db

def clean_and_load():
    print("--- Veri Okuma Basliyor (MinIO) ---")
    # MinIO (S3) Bağlantısı
    fs = s3fs.S3FileSystem(
        client_kwargs={'endpoint_url': MINIO_ENDPOINT},
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        use_listings_cache=False
    )
    
    # Dosyayı Oku
    file_path = "s3://dataops-bronze/raw/dirty_store_transactions.csv"
    with fs.open(file_path, 'rb') as f:
        df = pd.read_csv(f)
    
    print(f"Ham Veri Satir Sayisi: {len(df)}")
    
    # --- TEMIZLIK ---
    print("--- Temizlik Islemleri ---")
    
    # 1. Eksik verileri temizle (Örnek: Satır silme)
    df.dropna(inplace=True)
    
    # 2. Duplicate verileri temizle
    df.drop_duplicates(inplace=True)
    
    # 3. String temizliği (Örnek: Tutar sütununda '$' varsa kaldır ve float yap)
    # Veri setini görmedim ama genelde böyle olur. Gerekirse bu satırı düzenle:
    # df['amount'] = df['amount'].replace({'\$': '', ',': ''}, regex=True).astype(float)
    
    print(f"Temiz Veri Satir Sayisi: {len(df)}")
    
    # --- YUKLEME (Postgres) ---
    print("--- Postgres'e Yaziliyor (Full Load) ---")
    engine = create_engine(POSTGRES_CONN)
    
    # 'replace' kullanarak Full Load yapiyoruz (Eskiyi sil, yeniyi yaz)
    df.to_sql('clean_data_transactions', engine, schema='public', if_exists='replace', index=False)
    
    print("--- Islem Basariyla Tamamlandi ---")

if __name__ == "__main__":
    clean_and_load()
    
    