import streamlit as st
from supabase import create_client, Client
import pandas as pd
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import time

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

response = supabase.table('events').select('*').execute()
events_df = pd.DataFrame(response.data)
st.subheader('Таблица забегов из базы данных')
st.write(f'На текущий момент там {len(events_df)} записей')
st.dataframe(events_df.head())

EVENTS_URL = 'https://5verst.ru/events/'
MAX_CONCURRENT_REQUESTS = 30  
TIMEOUT_SECONDS = 60 

async def fetch(session, url):
    async with session.get(url, timeout=TIMEOUT_SECONDS) as response:
        return await response.text()

async def fetch_events(session):
    html = await fetch(session, EVENTS_URL)
    soup = BeautifulSoup(html, 'html.parser')
    events = [
        {
            'city': block.find('h4').get_text(strip=True),
            'location': location.find('a').get_text(strip=True),
            'info': location.get_text(strip=True).replace(location.find('a').text, '').strip(),
            'location_link': location.find('a')['href'],
            'location_results_link': location.find('a')['href'] + '/results/all/'
        }
        for block in soup.find_all('div', class_='event-block')
        for location in block.find_all('li')
    ]
    return events

async def parse_run_details(session, run, semaphore):
    async with semaphore:  
        run_link = run['location_results_link']
        html = await fetch(session, run_link)
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        run_details = [
            {
                'run': run['location'] + ' #' + columns[0].get_text(strip=True),
                'date': columns[1].get_text(strip=True),
                'link': columns[1].find('a')['href'] if columns[1].find('a') else None,
                'finishers': columns[2].get_text(strip=True),
                'volunteers': columns[3].get_text(strip=True),
                'avg_time': columns[4].get_text(strip=True),
                'best_female_time': columns[5].get_text(strip=True),
                'best_male_time': columns[6].get_text(strip=True)
            }
            for row in table.find_all('tr')[1:]
            if (columns := row.find_all('td')) and columns[0].get_text(strip=True) != ''
        ]
        return run_details

st.subheader('Обновлённые забеги')

async def main():
    start_time = time.time()  
    async with aiohttp.ClientSession() as session:
        events = await fetch_events(session)
        st.write(f'Найдено локаций: {len(events)}')
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        tasks = [parse_run_details(session, event, semaphore) for event in events]
        results = await asyncio.gather(*tasks)
        updated_events = [detail for sublist in results for detail in sublist]
        # updated_events = [event for event in updated_events if event['run'].startswith('Петергоф Александрийский')] # фильтр на хоумранс

    updated_events_df = pd.DataFrame(updated_events)
    st.write(f'Новое количество проведённых мероприятий: {len(updated_events_df)}')
    st.dataframe(updated_events_df.head())

    st.subheader('Забеги, которые были проведены и ещё не попали в базу данных')
    updated_events_not_in_db = [event for event in updated_events_df['run'].unique() if event not in events_df['run'].unique()]
    st.write(len(updated_events_not_in_db))
    st.write(updated_events_not_in_db)
    
    end_time = time.time()  
    elapsed_time = end_time - start_time 
    st.write(f'Время выполнения {elapsed_time:.2f} секунд')

if __name__ == '__main__':
    asyncio.run(main())