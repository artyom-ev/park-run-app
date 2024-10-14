import os
import streamlit as st
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd  
import aiohttp
import asyncio
from aiohttp import ClientTimeout
from asyncio import Semaphore

#####################################################################################################################################################
# Настройка страницы
#####################################################################################################################################################

# Конфигурация страницы
st.set_page_config(page_title='PARK🌳RUN', page_icon=':running:')

# Путь к изображению
image_path = 'logo.jpg'

# Вставка изображения
st.image(image_path, caption='', width=250)

# Скрытие футера и меню
hide_streamlit_style = """
            <style>
            MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# Заголовок
st.title('Домашняя страница')

st.subheader('Описание функционала и список страниц')

st.markdown('''
Тут возможно будет какое-то описание функционала приложения.  
Список страниц:
- Основная таблица
- Таблица рекордов
- Страница с таблицей бегунов
- Страница с таблицей организаторов
''')

#####################################################################################################################################################
# Парсинг
#####################################################################################################################################################
main_url = 'https://5verst.ru/results/latest/'
target_runs = ['Петергоф Александрийский']

# Устанавливаем таймаут и максимальное количество параллельных запросов
MAX_CONCURRENT_REQUESTS = 10  # Максимум 10 запросов одновременно
TIMEOUT_SECONDS = 30  # Таймаут на каждый запрос - 30 секунд

# Асинхронная функция для получения HTML страницы с таймаутом
async def fetch(session, url, semaphore):
    async with semaphore:  # Ограничиваем количество параллельных запросов
        try:
            async with session.get(url, timeout=ClientTimeout(total=TIMEOUT_SECONDS)) as response:
                return await response.text()
        except asyncio.TimeoutError:
            print(f"Timeout for {url}")
            return None

# Асинхронная функция для парсинга основной таблицы результатов
async def parse_main_table(session, url, semaphore):
    response_text = await fetch(session, url, semaphore)
    if response_text is None:
        return []

    soup = BeautifulSoup(response_text, 'html.parser')
    table = soup.find('table')
    starts_latest = []

    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        number_cell = cells[0]
        run = number_cell.text.strip().split(' #')[0]
        link = number_cell.find('a')['href'] if number_cell.find('a') else None
        date = cells[1].text.strip()
        starts_latest.append([run, date, link])

    return starts_latest

# Асинхронная функция для парсинга страницы с забегом
async def parse_run_page(session, run_link, location_name, semaphore):
    response_text = await fetch(session, run_link, semaphore)
    if response_text is None:
        return []

    soup = BeautifulSoup(response_text, 'html.parser')
    run_table = soup.find('table')
    run_data = []

    for run_row in run_table.find_all('tr')[1:]:
        run_cells = run_row.find_all('td')
        number = run_cells[0].get_text(strip=True)
        date_cell = run_cells[1].get_text(strip=True)
        link = run_cells[1].find('a')['href'] if run_cells[1].find('a') else None
        finishers = int(run_cells[2].get_text(strip=True))
        volunteers = int(run_cells[3].get_text(strip=True))
        avg_time = run_cells[4].get_text(strip=True)
        best_female_time = run_cells[5].get_text(strip=True)
        best_male_time = run_cells[6].get_text(strip=True)

        run_data.append([location_name, number, date_cell, link, finishers, volunteers, avg_time, best_female_time, best_male_time])

    return run_data

# Асинхронная функция для парсинга участников и волонтёров
async def parse_participant_and_volunteer_tables(session, run_protocol_link, run_info, semaphore):
    response_text = await fetch(session, run_protocol_link, semaphore)
    if response_text is None:
        return [], []

    soup = BeautifulSoup(response_text, 'html.parser')
    all_tables = soup.find_all('table')

    # Забег: location_name, number, date_cell, link, finishers, volunteers, avg_time, best_female_time, best_male_time
    location_name, number, date_cell, link, finishers, volunteer_count, avg_time, best_female_time, best_male_time = run_info

    participants_data = []
    volunteers_data = []

    # Парсим участников
    participant_table = all_tables[0]
    for row in participant_table.find_all('tr')[1:]:
        cells = row.find_all('td')
        if len(cells) >= 4:
            position = cells[0].get_text(strip=True)
            name_tag = cells[1].find('a')
            name = name_tag.get_text(strip=True) if name_tag else '—'
            profile_link = name_tag['href'] if name_tag else '—'
            participant_id = profile_link.split('/')[-1] if profile_link != '—' else '—'
            stats_div = cells[1].find('div', class_='user-stat')
            finishes = '—'
            volunteers = '—'
            if stats_div:
                stats_spans = stats_div.find_all('span')
                finishes = stats_spans[0].get_text(strip=True).split(' ')[0] if len(stats_spans) > 0 else '—'
                volunteers = stats_spans[1].get_text(strip=True).split(' ')[0] if len(stats_spans) > 1 else '—'
            club_tags = cells[1].find_all('span', class_='club-icon')
            clubs = ', '.join([club['title'] for club in club_tags]) if club_tags else '—'
            age_group = cells[2].get_text(strip=True).split(' ')[0] if cells[2] else '—'
            age_grade_tag = cells[2].find('div', class_='age_grade')
            age_grade = age_grade_tag.get_text(strip=True) if age_grade_tag else '—'
            time = cells[3].get_text(strip=True) if cells[3] else '—'
            achievements = []
            achievements_div = cells[3].find('div', class_='table-achievments')
            if achievements_div:
                achievement_icons = achievements_div.find_all('span', class_='results_icon')
                for icon in achievement_icons:
                    achievements.append(icon['title'])
            participants_data.append([location_name, number, date_cell, link, finishers, volunteer_count, avg_time, best_female_time, best_male_time,
                                      position, name, profile_link, participant_id, clubs, finishes, volunteers, age_group, age_grade, time, ', '.join(achievements)])
            
    # Парсим волонтёров
    volunteer_table = all_tables[1]
    for row in volunteer_table.find_all('tr')[1:]:
        columns = row.find_all('td')
        if len(columns) > 1:
            name_tag = columns[0].find('a')
            name = name_tag.get_text(strip=True) if name_tag else '—'
            profile_link = name_tag['href'] if name_tag else '—'
            participant_id = profile_link.split('/')[-1] if profile_link != '—' else '—'
            stats_div = columns[0].find('div', class_='user-stat')
            finishes = '—'
            volunteers = '—'
            if stats_div:
                stats_spans = stats_div.find_all('span')
                finishes = stats_spans[0].get_text(strip=True).split(' ')[0] if len(stats_spans) > 0 else '—'
                volunteers = stats_spans[1].get_text(strip=True).split(' ')[0] if len(stats_spans) > 1 else '—'
            club_tags = columns[0].find_all('span', class_='club-icon')
            clubs = ', '.join([club['title'] for club in club_tags]) if club_tags else '—'
            volunteer_role_info = columns[1].find('div', class_='volunteer__role')
            if volunteer_role_info:
                first_volunteer_tag = volunteer_role_info.find('span', class_='results_icon')
                first_volunteer_info = first_volunteer_tag['title'] if first_volunteer_tag else '—'
                role_tag = volunteer_role_info.find_all('span')
                volunteer_role = role_tag[-1].get_text(strip=True) if role_tag else '—'
            else:
                first_volunteer_info = '—'
                volunteer_role = '—'
            volunteers_data.append([location_name, number, date_cell, link, finishers, volunteer_count, avg_time, best_female_time, best_male_time,
                                    name, profile_link, participant_id, finishes, volunteers, clubs, volunteer_role, first_volunteer_info])
    
    return participants_data, volunteers_data

# Основная асинхронная функция для получения и обработки данных забегов
async def get_full_run_data(main_url, target_runs):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # Создаём семафор

    async with aiohttp.ClientSession() as session:
        # Парсим основную таблицу
        starts_latest = await parse_main_table(session, main_url, semaphore)
        filtered_runs = [run for run in starts_latest if any(location in run[0] for location in target_runs)]

        all_participant_data = []
        all_volunteer_data = []

        # Асинхронно обрабатываем каждый забег
        for run in filtered_runs:
            run_link = run[2]
            location_name = run[0]
            run_data = await parse_run_page(session, run_link, location_name, semaphore)

            # Создаём задачи для получения данных участников и волонтёров
            tasks = [
                parse_participant_and_volunteer_tables(session, run_info[3], run_info, semaphore)
                for run_info in run_data if run_info[3]
            ]
            results = await asyncio.gather(*tasks)

            # Собираем результаты
            for participants, volunteers in results:
                all_participant_data.extend(participants)
                all_volunteer_data.extend(volunteers)

        return all_participant_data, all_volunteer_data
    

# Асинхронная функция для парсинга страницы участника
async def parse_participant_page(participant_link, session, semaphore):
    html = await fetch(session, participant_link, semaphore)
    if html is None:
        return []  # Возвращаем пустой результат в случае ошибки
    soup = BeautifulSoup(html, 'html.parser')
    stats_data = []

    # Найдём div с нужной структурой
    stats_div = soup.find('div', class_='grid grid-cols-2 gap-px bg-black/[0.05]')
    if stats_div:
        stats_items = stats_div.find_all('div', class_='bg-white p-4')

        finishes = stats_items[0].find('span', class_='text-3xl font-semibold tracking-tight').text.strip() if len(stats_items) > 0 else 'N/A'
        volunteers = stats_items[1].find('span', class_='text-3xl font-semibold tracking-tight').text.strip() if len(stats_items) > 1 else 'N/A'
        best_time = stats_items[2].find('span', class_='text-3xl font-semibold tracking-tight').text.strip() if len(stats_items) > 2 else 'N/A'
        best_time_link = stats_items[2].find('a', class_='user-info-park-link')['href'] if len(stats_items) > 2 and stats_items[2].find('a', class_='user-info-park-link') else 'N/A'

        clubs = stats_items[3].find_all('span', class_='club-icon') if len(stats_items) > 3 else []
        clubs_titles = ', '.join([club['title'] for club in clubs])
    else:
        finishes = volunteers = best_time = best_time_link = 'N/A'
        clubs_titles = ''

    # Найдём таблицы для забегов в Петергофе
    tables = soup.find_all('table')
    if tables:
        # Подсчёт финишей в Петергофе
        peterhof_finishes_count = sum(1 for row in tables[0].find_all('tr')[1:] if 'Петергоф Александрийский' in row.find_all('td')[1].text.strip()) if len(tables) > 0 else 0

        # Для волонтёрств создаём множество для уникальных дат
        peterhof_volunteer_dates = set()
        if len(tables) > 1:
            for row in tables[1].find_all('tr')[1:]:
                location = row.find_all('td')[1].text.strip()
                if 'Петергоф Александрийский' in location:
                    date = row.find_all('td')[0].text.strip()  # Извлекаем дату
                    peterhof_volunteer_dates.add(date)  # Добавляем дату в множество (уникальные даты)
        peterhof_volunteers_count = len(peterhof_volunteer_dates)  # Количество уникальных дат волонтёрств
    else:
        peterhof_finishes_count = peterhof_volunteers_count = 0

    # Возвращаем собранные данные
    stats_data.append([best_time, finishes, peterhof_finishes_count, volunteers, peterhof_volunteers_count, clubs_titles, best_time_link])
    return stats_data

# Основная функция для сбора данных
async def get_all_stats_data(df_runners):
    # Инициализируем список для хранения данных
    all_stats_data = []

    # Пройдемся по каждому уникальному участнику
    unique_links = [link for link in df_runners['profile_link'].unique() if '5verst.ru/userstats' in link]

    semaphore = Semaphore(MAX_CONCURRENT_REQUESTS)  # Ограничение на количество параллельных запросов

    async with aiohttp.ClientSession() as session:
        tasks = []
        for link in unique_links:
            tasks.append(parse_participant_page(link, session, semaphore))

        # Запускаем парсинг всех участников параллельно
        results = await asyncio.gather(*tasks)

        # Собираем результат
        for link, parsed_data in zip(unique_links, results):
            if parsed_data:  # Если данные были успешно получены
                runner_row = df_runners[df_runners['profile_link'] == link].iloc[0]
                name = runner_row['name']
                profile_link = runner_row['profile_link']
                participant_id = runner_row['participant_id']

                # Добавляем данные в общий список, включая данные из таблицы df_runners
                for data in parsed_data:
                    all_stats_data.append([name, profile_link, participant_id] + data)

    # Возвращаем итоговый список данных
    return all_stats_data

#####################################################################################################################################################
# Сохранение в Базу Данных
#####################################################################################################################################################

# Функция для сохранения данных в базу данных
def save_to_database(df_runners, df_orgs, df_stats, db_url='sqlite:///mydatabase.db'):
    # Создаем подключение к базе данных
    engine = create_engine(db_url)
    # Сохраняем данные бегунов в таблицу 'runners'
    df_runners.to_sql('runners', con=engine, if_exists='replace', index=False)
    # Сохраняем данные организаторов в таблицу 'organizers'
    df_orgs.to_sql('organizers', con=engine, if_exists='replace', index=False)
    # Сохраняем данные бегунов в таблицу 'runners'
    df_stats.to_sql('users', con=engine, if_exists='replace', index=False)

# Основная функция получения и сохранения в бд
async def update_data():
    # Получаем данные о всех участниках и волонтёрах
    all_participant_data, all_volunteer_data = await get_full_run_data(main_url, target_runs)

    # Создаём DataFrame для бегунов
    df_runners = pd.DataFrame(all_participant_data, columns=[
        'run', 'run_number', 'run_date', 'run_link', 'finisher', 'volunteer', 'avg_time',
        'best_female_time', 'best_male_time', 'position', 'name', 'profile_link',
        'participant_id', 'clubs', 'finishes', 'volunteers', 'age_group', 'age_grade',
        'time', 'achievements'
    ])
    df_runners['run_date'] = pd.to_datetime(df_runners['run_date'], dayfirst=True)

    # Создаём DataFrame для волонтёров
    df_orgs = pd.DataFrame(all_volunteer_data, columns=[
        'run', 'run_number', 'run_date', 'run_link', 'finisher', 'volunteer', 'avg_time',
        'best_female_time', 'best_male_time', 'name', 'profile_link', 'participant_id',
        'finishes', 'volunteers', 'clubs', 'volunteer_role', 'first_volunteer_info'
    ])
    df_orgs = (
        df_orgs.groupby([col for col in df_orgs.columns if col != 'volunteer_role'])['volunteer_role']
        .apply(lambda x: ', '.join(x))
        .reset_index()
    )
    df_orgs['run_date'] = pd.to_datetime(df_orgs['run_date'], dayfirst=True)

    # Получаем статистические данные
    all_stats_data = await get_all_stats_data(df_runners)

    # Создаём DataFrame для итоговой статистики
    df_stats = pd.DataFrame(all_stats_data, columns=[
        'name', 'profile_link', 'participant_id', 'best_time', 'finishes', 
        'peterhof_finishes_count', 'volunteers', 'peterhof_volunteers_count', 
        'clubs_titles', 'best_time_link'
    ])

    # Сохраняем данные в базу данных
    save_to_database(df_runners, df_orgs, df_stats)

#####################################################################################################################################################
# Проверка актуальности данных
#####################################################################################################################################################

# Функция для получения последней даты из сайта
def get_last_date_from_site():
    url = 'https://5verst.ru/petergofaleksandriysky/results/all/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    last_date = soup.find_all('table')[0].find_all('tr')[1].find_all('td')[1].text.strip()

    # Преобразование last_date из формата DD.MM.YYYY в объект datetime
    last_date_site = datetime.strptime(last_date, '%d.%m.%Y').date()
    return last_date_site

# Функция для получения последней даты из БД
def get_last_date_from_db(db_url='sqlite:///mydatabase.db'):
    # Проверяем, существует ли файл базы данных
    db_path = db_url.replace('sqlite:///', '')  # Извлекаем путь к файлу базы данных
    if not os.path.exists(db_path):
        return None  # Если базы данных нет, возвращаем None
    
    
    # Подключение к базе данных, если файл существует
    engine = create_engine(db_url)
    with engine.connect() as connection:
        query = text("SELECT MAX(run_date) FROM runners")  # Заменить run_date на реальное имя колонки с датой
        result = connection.execute(query)
        last_date_db = result.scalar()

        # Проверяем, если last_date_db не None, то преобразуем строку в дату
        if last_date_db:
            last_date_db = datetime.strptime(last_date_db, '%Y-%m-%d %H:%M:%S.%f').date()
        else:
            last_date_db = None
    return last_date_db


last_date_site = get_last_date_from_site()
last_date_db = get_last_date_from_db()
    
st.subheader('Актуальность данных')  
# Проверяем, если last_date_db не пустая
if last_date_db is None:
    st.write('Данных в базе нет!')
    if st.button('Обновить данные'):
        st.write('Начинаем парсинг данных...')
        asyncio.run(update_data())
        st.success('Данные успешно сохранены в базу данных!')
else:
    # Сравнение дат
    if last_date_db != last_date_site:
        st.write(f'Данные устарели. Дата в базе: {last_date_db}, дата на сайте: {last_date_site}.')
        
        if st.button('Обновить данные'):
            st.write('Начинаем парсинг данных...')
            asyncio.run(update_data())
            st.success('Данные успешно сохранены в базу данных!')
    else:
        st.markdown(f'''Данные актуальны 👍  
                    Последняя дата в базе данных: {last_date_db}  
                    Последняя дата на сайте: {last_date_site}
                    ''')

#####################################################################################################################################################
# Поиск по имени
#####################################################################################################################################################

# st.subheader('Поиск по имени')        
# # Функция для поиска по базе данных
# def search_database(query):
#     db_url = 'sqlite:///mydatabase.db'
#     engine = create_engine(db_url)
#     with engine.connect() as connection:
#         # Используем SQL для поиска
#         query_like = f"%{query}%"
#         sql_query = text("SELECT * FROM runners WHERE name LIKE :query")
#         result = connection.execute(sql_query, {"query": query_like}).fetchall()  # Передаем параметры как словарь
#     return result

# # Поле для поиска
# search_query = st.text_input("Поиск по имени бегуна:")

# # Обработка поискового запроса
# if search_query:
#     search_results = search_database(search_query)

#     if search_results:
#         # Преобразуем результаты в DataFrame
#         df_results = pd.DataFrame(search_results)  # Без указания имен колонок
#         st.dataframe(df_results)  # Отображаем результаты в виде таблицы
#     else:
#         st.write("Нет результатов по вашему запросу.")