# CREATE TABLE volunteers (
#     id SERIAL PRIMARY KEY,
#     run VARCHAR(200),
#     date VARCHAR(50),
#     link VARCHAR(200),
#     finishers VARCHAR(50),
#     volunteers VARCHAR(50),
#     avg_time VARCHAR(50),
#     best_female_time VARCHAR(50),
#     best_male_time VARCHAR(50),
#     name VARCHAR(200),
#     profile_link VARCHAR(200),
#     participant_id VARCHAR(200),
#     n_finishes VARCHAR(50),
#     n_volunteers VARCHAR(50),
#     clubs VARCHAR(200),
#     first_volunteer VARCHAR(50),
#     volunteer_role VARCHAR(200)
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

volunteers = load_json(DATA_DIR / 'volunteers.json')
print(f'Всего записей: {len(volunteers)}')

chunk_size = 10000
total_chunks = (len(volunteers) + chunk_size - 1) // chunk_size

for i in tqdm(range(0, len(volunteers), chunk_size), total=total_chunks, desc='Вставка данных'):
    chunk = volunteers[i:i + chunk_size]
    response = supabase.table('volunteers').insert(chunk).execute()

print(f'Все данные успешно вставлены в таблицу')

end_time = time.time() 
elapsed_time = end_time - start_time  
print(f'Время выполнения: {elapsed_time:.2f} секунд')