# CREATE TABLE events (
#     id SERIAL PRIMARY KEY,
#     run VARCHAR(200),
#     date VARCHAR(50),
#     link VARCHAR(200),
#     finishers VARCHAR(50),
#     volunteers VARCHAR(50),
#     avg_time VARCHAR(50),
#     best_female_time VARCHAR(50),
#     best_male_time VARCHAR(50)
# );

import json
from pathlib import Path
from supabase import create_client, Client
import streamlit as st
from tqdm import tqdm
import time

BASE_DIR = Path(__file__).resolve().parent.parent  
DATA_DIR = BASE_DIR / 'data'  
SUPABASE_URL = st.secrets['SUPABASE_URL']
SUPABASE_KEY = st.secrets['SUPABASE_KEY']
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def load_json(filepath: Path):
    with open(filepath, 'r', encoding='utf8') as json_file:
        return json.load(json_file)

start_time = time.time()
print('Начало выполнения...')

events = load_json(DATA_DIR / 'events.json')
print(f'Всего записей: {len(events)}')

chunk_size = 10000
total_chunks = (len(events) + chunk_size - 1) // chunk_size

for i in tqdm(range(0, len(events), chunk_size), total=total_chunks, desc='Вставка данных'):
    chunk = events[i:i + chunk_size]
    response = supabase.table('events').insert(chunk).execute()

print(f'Все данные успешно вставлены в таблицу')

end_time = time.time() 
elapsed_time = end_time - start_time  
print(f'Время выполнения: {elapsed_time:.2f} секунд')