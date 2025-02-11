# CREATE TABLE participants (
#     id SERIAL PRIMARY KEY,
#     run VARCHAR(200),
#     date VARCHAR(50),
#     link VARCHAR(200),
#     finishers VARCHAR(50),
#     volunteers VARCHAR(50),
#     avg_time VARCHAR(50),
#     best_female_time VARCHAR(50),
#     best_male_time VARCHAR(50),
#     position VARCHAR(50),
#     name VARCHAR(200),
#     profile_link VARCHAR(200),
#     participant_id VARCHAR(200),
#     n_finishes VARCHAR(50),
#     n_volunteers VARCHAR(50),
#     clubs VARCHAR(200),
#     age_group VARCHAR(50),
#     age_grade VARCHAR(50),
#     time VARCHAR(50),
#     achievements VARCHAR(200)
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

participants = load_json(DATA_DIR / 'participants.json')
print(f'Всего записей: {len(participants)}')

chunk_size = 10000
total_chunks = (len(participants) + chunk_size - 1) // chunk_size

for i in tqdm(range(0, len(participants), chunk_size), total=total_chunks, desc='Вставка данных'):
    chunk = participants[i:i + chunk_size]
    response = supabase.table('participants').insert(chunk).execute()

print(f'Все данные успешно вставлены в таблицу')

end_time = time.time() 
elapsed_time = end_time - start_time  
print(f'Время выполнения: {elapsed_time:.2f} секунд')