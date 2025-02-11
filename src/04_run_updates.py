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
st.subheader('Информация по базе:')
st.write(f'В базе данных количество проведенных мероприятий составляет: {len(events_df)} записей')

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

async def parse_participants_and_volunteers(session, run_link):
    html = await fetch(session, run_link)
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')
    
    if not tables:
        st.write(f'Ошибка: на странице {run_link} не найдено таблиц.')
        return [], []
    
    participant_table = tables[0]
    participants = [
        {
            'position': columns[0].get_text(strip=True),
            'name': (name_tag.get_text(strip=True) if (name_tag := columns[1].find('a')) else ''),
            'profile_link': (name_tag['href'] if name_tag else ''),
            'participant_id': (name_tag['href'].split('/')[-1] if name_tag and name_tag['href'] != '' else ''),
            'n_finishes': (stats_spans[0].get_text(strip=True).split(' ')[0].split('\xa0')[0] 
                            if (stats_div := columns[1].find('div', class_='user-stat')) 
                            and (stats_spans := stats_div.find_all('span')) else ''),
            'n_volunteers': (stats_spans[1].get_text(strip=True).split(' ')[0].split('\xa0')[0]  
                            if stats_div and len(stats_spans) > 1 else ''),
            'clubs': (', '.join([club['title'] for club in columns[1].find_all('span', class_='club-icon')]) 
                        if columns[1].find_all('span', class_='club-icon') else ''),
            'age_group': (columns[2].get_text(strip=True).split(' ')[0] if columns[2] else ''),
            'age_grade': (age_grade_tag.get_text(strip=True).split('%')[0] 
                            if (age_grade_tag := columns[2].find('div', class_='age_grade')) else ''),
            'time': (columns[3].get_text(strip=True) if columns[3] else ''),
            'achievements': (', '.join([icon['title'] for icon in achievements_div.find_all('span', class_='results_icon')]) 
                                if (achievements_div := columns[3].find('div', class_='table-achievments')) else '')
        }
        for row in participant_table.find_all('tr')[1:]
        if (columns := row.find_all('td'))
    ]
    
    if len(tables) < 2:
        st.write(f'Ошибка: на странице {run_link} отсутствует таблица волонтёров.')
        return participants, []
    
    volunteer_table = tables[1]
    volunteers = [
        {
            'name': (name_tag.get_text(strip=True) if (name_tag := columns[0].find('a')) else ''),
            'profile_link': (name_tag['href'] if name_tag else ''),
            'participant_id': (name_tag['href'].split('/')[-1] if name_tag and name_tag['href'] != '' else ''),
            'n_finishes': (stats_spans[0].get_text(strip=True).split(' ')[0].split('\xa0')[0] 
                            if (stats_div := columns[0].find('div', class_='user-stat')) 
                            and (stats_spans := stats_div.find_all('span')) else ''),
            'n_volunteers': (stats_spans[1].get_text(strip=True).split(' ')[0].split('\xa0')[0] 
                            if stats_div and len(stats_spans) > 1 else ''),
            'clubs': (', '.join([club['title'] for club in columns[0].find_all('span', class_='club-icon')]) 
                        if columns[0].find_all('span', class_='club-icon') else ''),
            'first_volunteer': (first_volunteer_tag['title'] if (volunteer_role_info := columns[1].find('div', class_='volunteer__role')) 
                                        and (first_volunteer_tag := volunteer_role_info.find('span', class_='results_icon')) else ''),
            'volunteer_role': (role_tag[-1].get_text(strip=True) if volunteer_role_info 
                                and (role_tag := volunteer_role_info.find_all('span')) else '')
        }
        for row in volunteer_table.find_all('tr')[1:]
        if (columns := row.find_all('td'))
    ]
    
    return participants, volunteers

def upload_to_db(table_name, data):
    try:
        response = supabase.table(table_name).insert(data).execute()
        return response
    except Exception as exception:
        return exception

async def main():
    start_time = time.time()  
    async with aiohttp.ClientSession() as session:
        events = await fetch_events(session)
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        tasks = [parse_run_details(session, event, semaphore) for event in events]
        results = await asyncio.gather(*tasks)
        updated_events = [detail for sublist in results for detail in sublist]
        # updated_events = [event for event in updated_events if event['run'].startswith('Петергоф Александрийский')] # фильтр на хоумранс

        st.subheader('Информация по сайту:')
        st.write(f'Сейчас общее количество проведенных мероприятий уже достигло: {len(updated_events)}')

        st.subheader('Новые мероприятия, которыми обновим базу данных:')
        new_events = [item for item in updated_events if item['run'] not in events_df['run'].unique()]
        st.write(f'Количество новых мероприятий: {len(new_events)}')
        st.write(f'Строки, которыми обновим таблицу events')
        st.write(new_events)
        upload_to_db('events', new_events)

        st.subheader('Парсинг новых участников и волонтёров...')   
        new_participants_data = []
        new_volunteers_data = []
        for event in new_events:
            participants, volunteers = await parse_participants_and_volunteers(session, event['link'])
            
            for participant in participants:
                participant_data = {
                    **event,  
                    **participant 
                }
                new_participants_data.append(participant_data)
            
            for volunteer in volunteers:
                volunteer_data = {
                    **event,  
                    **volunteer  
                }
                new_volunteers_data.append(volunteer_data)

        st.write(f'Строки, которыми обновим таблицу участников')
        st.write(new_participants_data)
        upload_to_db('participants', new_participants_data)

        st.write(f'Строки, которыми обновим таблицу волонтёров')
        st.write(new_volunteers_data)
        upload_to_db('volunteers', new_volunteers_data)

    end_time = time.time()  
    elapsed_time = end_time - start_time  
    st.write(f'Время выполнения: {elapsed_time:.2f} секунд')

if __name__ == '__main__':
    asyncio.run(main())