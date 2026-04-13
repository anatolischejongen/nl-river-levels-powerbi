import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

# --- YAPILANDIRMA (CONFIG) ---
REFERENCE_FILE = "../data/reference/stations.csv"
OUTPUT_FILE = "../data/raw/historical_water_levels.csv"
START_DATE = "2021-01-01"
END_DATE = "2023-12-31"

def generate_water_levels():
    print("🚀 Veri Mühendisliği Boru Hattı Başlatılıyor...")
    
    # 1. Referans dosyasının varlığını kontrol et
    if not os.path.exists(REFERENCE_FILE):
        print(f"❌ HATA: {REFERENCE_FILE} bulunamadı!")
        return

    # 2. İstasyonları oku
    stations_df = pd.read_csv(REFERENCE_FILE)
    print(f"✅ {len(stations_df)} istasyon başarıyla okundu.")

    # 3. Tarih aralığını oluştur (3 yıllık günlük veri)
    date_range = pd.date_range(start=START_DATE, end=END_DATE, freq='D')
    
    all_data = []

    # 4. Her istasyon için gerçekçi veri üret (Mock API Logic)
    for index, row in stations_df.iterrows():
        station_code = row['station_code']
        critical_level = row['critical_level_cm']
        
        print(f"   -> [{station_code}] için 3 yıllık veri çekiliyor/üretiliyor...")
        
        # Mevsimsellik simülasyonu (Sinüs dalgası: Kışın yüksek, Yazın düşük)
        # Günleri 0-365 arası sayılara çevir
        day_of_year = date_range.dayofyear
        
        # Temel su seviyesi (Kritik seviyenin biraz altı)
        base_level = critical_level * 0.6 
        
        # Mevsimsel dalgalanma (Kritik seviyenin %30'u kadar aşağı/yukarı oyna)
        seasonal_effect = np.sin((day_of_year - 30) * (2 * np.pi / 365)) * (critical_level * 0.3)
        
        # Rastgele gürültü (Günlük ufak değişimler)
        noise = np.random.normal(0, critical_level * 0.05, len(date_range))
        
        # Toplam su seviyesi hesapla
        water_levels = base_level + seasonal_effect + noise
        
        # Çok nadir görülen ekstrem "Taşkın" (Kritik eşiği aşan) olayları ekle (%1 ihtimal)
        flood_spikes = np.random.choice([0, critical_level * 0.2], size=len(date_range), p=[0.99, 0.01])
        water_levels += flood_spikes
        
        # Veriyi bir DataFrame'e dönüştür
        station_data = pd.DataFrame({
            'date': date_range,
            'station_code': station_code,
            'water_level_cm': np.round(water_levels, 0).astype(int)
        })
        
        all_data.append(station_data)

    # 5. Tüm verileri birleştir ve CSV'ye kaydet
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Raw klasörü yoksa oluştur
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"🎉 BAŞARILI! Toplam {len(final_df)} satır veri '{OUTPUT_FILE}' dosyasına kaydedildi.")

if __name__ == "__main__":
    generate_water_levels()