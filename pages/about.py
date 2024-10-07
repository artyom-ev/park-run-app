import os
import streamlit as st
import requests
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd  
import aiohttp
import asyncio

# Конфигурация страницы
st.set_page_config(page_title='PARK🌳RUN', page_icon=':running:')

# Путь к изображению
image_path = 'logo.jpg'  # Замените на путь к вашему изображению

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

# Функция для парсинга страницы с информацией по забегам локации
def parse_run_page(run_link):
    try:
        response = requests.get(run_link)
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        run_table = soup.find('table')

        run_data = []
        if run_table:
            for run_row in run_table.find_all('tr')[1:]:
                run_cells = run_row.find_all('td')
                if len(run_cells) >= 4:
                    # Извлекаем данные по каждому забегу
                    number = run_cells[0].get_text(strip=True)
                    date_cell = run_cells[1].get_text(strip=True)
                    link = run_cells[1].find('a')['href'] if run_cells[1].find('a') else None
                    finishers = run_cells[2].get_text(strip=True)
                    volunteers = run_cells[3].get_text(strip=True)
                    avg_time = run_cells[4].get_text(strip=True)
                    best_female_time = run_cells[5].get_text(strip=True)
                    best_male_time = run_cells[6].get_text(strip=True)
                    run_data.append([number, date_cell, link, finishers, volunteers, avg_time, best_female_time, best_male_time])
        return run_data
    except Exception as e:
        print(f"Ошибка при парсинге страницы {run_link}: {e}")
        return None

# Функция для парсинга таблиц участников и волонтёров для конкретного забега
def parse_participant_and_volunteer_tables(run_link):
    try:
        response = requests.get(run_link)
        if response.status_code != 200:
            return None, None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        all_tables = soup.find_all('table')

        if len(all_tables) < 2:
            return None, None  # Недостаточно таблиц

        # Первая таблица – участники
        participants_data = []
        participant_table = all_tables[0]
        for row in participant_table.find_all('tr')[1:]:
            cells = row.find_all('td')
            if len(cells) >= 4:
                position = cells[0].get_text(strip=True)
                name_tag = cells[1].find('a')
                name = name_tag.get_text(strip=True) if name_tag else '—'
                profile_link = name_tag['href'] if name_tag else '—'
                participant_id = profile_link.split('/')[-1] if profile_link != '—' else '—'
                # Количество финишей и волонтёрств
                stats_div = cells[1].find('div', class_='user-stat')
                finishes = '—'
                volunteers = '—'
                if stats_div:
                    stats_spans = stats_div.find_all('span')
                    finishes = stats_spans[0].get_text(strip=True).split(' ')[0] if len(stats_spans) > 0 else '—'
                    volunteers = stats_spans[1].get_text(strip=True).split(' ')[0] if len(stats_spans) > 1 else '—'

                # Клубы
                club_tags = cells[1].find_all('span', class_='club-icon')
                clubs = ', '.join([club['title'] for club in club_tags]) if club_tags else '—'

                # Возрастная группа и Age Grade
                age_group = cells[2].get_text(strip=True).split(' ')[0] if cells[2] else '—'
                age_grade_tag = cells[2].find('div', class_='age_grade')
                age_grade = age_grade_tag.get_text(strip=True) if age_grade_tag else '—'

                # Время и достижения
                time = cells[3].get_text(strip=True) if cells[3] else '—'
                achievements = []
                achievements_div = cells[3].find('div', class_='table-achievments')
                if achievements_div:
                    achievement_icons = achievements_div.find_all('span', class_='results_icon')
                    for icon in achievement_icons:
                        achievements.append(icon['title'])  # Описание достижения
                participants_data.append([position, name, profile_link, participant_id, clubs, finishes, volunteers, age_group, age_grade, time, ', '.join(achievements)])

        # Вторая таблица – волонтёры
        volunteers_data = []
        volunteer_table = all_tables[1]
        for row in volunteer_table.find_all('tr')[1:]:
            columns = row.find_all('td')
            if len(columns) > 1:
                name_tag = columns[0].find('a')
                name = name_tag.get_text(strip=True) if name_tag else '—'
                profile_link = name_tag['href'] if name_tag else '—'
                participant_id = profile_link.split('/')[-1] if profile_link != '—' else '—'
                                    # Извлекаем количество финишей и волонтёрств
                stats_div = columns[0].find('div', class_='user-stat')
                finishes = '—'
                volunteers = '—'
                if stats_div:
                    stats_spans = stats_div.find_all('span')
                    finishes = stats_spans[0].get_text(strip=True).split(' ')[0] if len(stats_spans) > 0 else '—'
                    volunteers = stats_spans[1].get_text(strip=True).split(' ')[0] if len(stats_spans) > 1 else '—'

                # Извлекаем клубы (все клубы)
                club_tags = columns[0].find_all('span', class_='club-icon')
                clubs = ', '.join([club['title'] for club in club_tags]) if club_tags else '—'

                # Вторая колонка: роль волонтёра и информация о первом волонтёрстве
                volunteer_role_info = columns[1].find('div', class_='volunteer__role')
                if volunteer_role_info:
                    # Извлекаем атрибут title для информации о первом волонтёрстве
                    first_volunteer_tag = volunteer_role_info.find('span', class_='results_icon')
                    first_volunteer_info = first_volunteer_tag['title'] if first_volunteer_tag else '—'

                    # Извлекаем текст для роли волонтёра
                    role_tag = volunteer_role_info.find_all('span')
                    volunteer_role = role_tag[-1].get_text(strip=True) if role_tag else '—'
                else:
                    first_volunteer_info = '—'
                    volunteer_role = '—'
                volunteers_data.append([name, profile_link, participant_id, finishes, volunteers, clubs, volunteer_role, first_volunteer_info])

        return participants_data, volunteers_data
    except Exception as e:
        print(f"Ошибка при парсинге таблиц участников и волонтёров: {e}")
        return None, None

# Многопоточная обработка данных для забегов и участников
def process_run_data(filtered_starts_latest):
    orgs_data = []
    runners_data = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(parse_run_page, row['run_link']) for _, row in filtered_starts_latest.iterrows()]

        for future, row in zip(futures, filtered_starts_latest.itertuples()):
            run_data = future.result()
            if run_data:
                for item in run_data:
                    # Переход на страницу конкретного забега
                    run_link = item[2]
                    participants, volunteers = parse_participant_and_volunteer_tables(run_link)

                    if participants:
                        for p in participants:
                            # Добавляем данные участника
                            runners_data.append([
                                row.run, item[0], item[1], run_link, item[3], item[4], item[5], item[6], item[7]
                            ] + p)

                    if volunteers:
                        for v in volunteers:
                            # Добавляем данные волонтёра
                            orgs_data.append([
                                row.run, item[0], item[1], run_link, item[3], item[4], item[5], item[6], item[7]
                            ] + v)

    return orgs_data, runners_data

# Функция для парсинга основной таблицы забегов
def parse_main_table(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Находим таблицу с результатами
    table = soup.find('table')

    # Список для хранения данных
    starts_latest = []
    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        if len(cells) > 0:
            number_cell = cells[0]
            run = number_cell.text.strip().split(' #')[0]  # Старт
            link = number_cell.find('a')['href'] if number_cell.find('a') else None
            date = cells[1].text.strip()  # Дата забега
            finishers = cells[2].text.strip()  # Число финишёров
            volunteers = cells[3].text.strip()  # Число волонтёров
            avg_time = cells[4].text.strip()  # Среднее время
            best_female_time = cells[5].text.strip()  # Лучшее время "Ж"
            best_male_time = cells[6].text.strip()  # Лучшее время "М"
            starts_latest.append([run, date, link, finishers, volunteers, avg_time, best_female_time, best_male_time])

    return pd.DataFrame(starts_latest, columns=[
        'run', 'run_date', 'run_link', 'finishers', 'volunteers', 'avg_time', 'best_female_time', 'best_male_time'
    ])

# Главная функция для парсинга сайта
def parse_website():
    url = 'https://5verst.ru/results/latest/'
    
    # Парсим основную таблицу с последними забегами
    starts_latest_df = parse_main_table(url)

    # Фильтрация забегов по нужным названиям
    target_runs = ['Петергоф Александрийский']  # Замените по необходимости
    filtered_starts_latest = starts_latest_df[starts_latest_df['run'].isin(target_runs)]

    # Обработка данных забегов и участников
    orgs_data, runners_data = process_run_data(filtered_starts_latest)
    
    return orgs_data, runners_data

# Функция для сохранения данных в базу данных
def save_to_database(df_orgs, df_runners, df_stats, db_url='sqlite:///mydatabase.db'):
    # Создаем подключение к базе данных
    engine = create_engine(db_url)

    # Сохраняем данные организаторов в таблицу 'organizers'
    df_orgs.to_sql('organizers', con=engine, if_exists='replace', index=False)

    # Сохраняем данные бегунов в таблицу 'runners'
    df_runners.to_sql('runners', con=engine, if_exists='replace', index=False)
    
    # Сохраняем данные бегунов в таблицу 'runners'
    df_stats.to_sql('users', con=engine, if_exists='replace', index=False)

async def fetch_profile_data(session, url, df_runners):
    async with session.get(url) as response:
        html = await response.text()
        soup = BeautifulSoup(html, 'html.parser')
        
        # Парсим данные как и раньше
        row = df_runners[df_runners['profile_link'] == url].iloc[0]  
        first_name = row['name'].split()[0]
        last_name = row['name'].split()[1]
        participant_id = row['participant_id']
        profile_link = row['profile_link']

        stats_div = soup.find('div', class_='grid grid-cols-2 gap-px bg-black/[0.05]')
        finishes = stats_div.find_all('div', class_='bg-white p-4')[0].find('span', class_='text-3xl font-semibold tracking-tight').text.strip()
        volunteers = stats_div.find_all('div', class_='bg-white p-4')[1].find('span', class_='text-3xl font-semibold tracking-tight').text.strip()
        best_time = stats_div.find_all('div', class_='bg-white p-4')[2].find('span', class_='text-3xl font-semibold tracking-tight').text.strip()
        best_time_link = stats_div.find_all('div', class_='bg-white p-4')[2].find('a', class_='user-info-park-link')['href']

        clubs = stats_div.find_all('div', class_='bg-white p-4')[3].find_all('span', class_='club-icon')
        clubs_titles = ', '.join([club['title'] for club in clubs])

        tables = soup.find_all('table')
        peterhof_finishes_count = sum(1 for row in tables[0].find_all('tr')[1:] if 'Петергоф Александрийский' in row.find_all('td')[1].text.strip())
        peterhof_volunteers_count = sum(1 for row in tables[1].find_all('tr')[1:] if 'Петергоф Александрийский' in row.find_all('td')[1].text.strip())
        
        return [participant_id, profile_link, first_name, last_name, best_time, finishes, 
                peterhof_finishes_count, volunteers, peterhof_volunteers_count, clubs_titles, best_time_link]

async def gather_profiles_data(urls, df_runners):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            if 'https://5verst.ru/userstats/' in url:
                task = fetch_profile_data(session, url, df_runners)
                tasks.append(task)
        return await asyncio.gather(*tasks)

# Основная функция для запуска
def run_parsing_async():
    orgs_data, runners_data = parse_website()

    # Создаем DataFrame для организаторов
    df_orgs = pd.DataFrame(orgs_data, columns=[
        'run', 'run_number', 'run_date', 'run_link', 'finisher', 'volunteer', 'avg_time', 'best_female_time', 'best_male_time', 
        'name', 'profile_link', 'participant_id', 'finishes', 'volunteers', 'clubs', 'volunteer_role', 'first_volunteer_info'
    ])
    df_orgs['run_date'] = pd.to_datetime(df_orgs['run_date'], dayfirst=True)
    df_orgs['finisher'] = df_orgs['finisher'].astype('int')
    df_orgs['volunteer'] = df_orgs['volunteer'].astype('int')

    # Создаем DataFrame для бегунов
    df_runners = pd.DataFrame(runners_data, columns=[
        'run', 'run_number', 'run_date', 'run_link', 'finisher', 'volunteer', 'avg_time', 'best_female_time', 'best_male_time', 
        'position', 'name', 'profile_link', 'participant_id', 'clubs', 'finishes', 'volunteers', 'age_group', 'age_grade', 'time', 'achievements'
    ])
    df_runners['run_date'] = pd.to_datetime(df_runners['run_date'], dayfirst=True)
    df_runners['finisher'] = df_runners['finisher'].astype('int')
    df_runners['volunteer'] = df_runners['volunteer'].astype('int')

    urls = df_runners['profile_link'].unique()

    # Запуск асинхронного парсинга
    stats_data = asyncio.run(gather_profiles_data(urls, df_runners))

    df_stats = pd.DataFrame(stats_data, columns=[
        'participant_id', 'profile_link', 'first_name', 'last_name', 'best_time', 'finishes', 
        'peterhof_finishes_count', 'volunteers', 'peterhof_volunteers_count', 'clubs', 'best_time_link'
    ])

    save_to_database(df_orgs, df_runners, df_stats)


last_date_site = get_last_date_from_site()
last_date_db = get_last_date_from_db()
    
st.subheader('Актуальность данных')  
# Проверяем, если last_date_db не пустая
if last_date_db is None:
    st.write('Данных в базе нет!')
    if st.button('Обновить данные'):
        st.write('Начинаем парсинг данных...')
        run_parsing_async()
        st.success('Данные успешно сохранены в базу данных!')
else:
    # Сравнение дат
    if last_date_db != last_date_site:
        st.write(f'Данные устарели. Дата в базе: {last_date_db}, дата на сайте: {last_date_site}.')
        
        if st.button('Обновить данные'):
            st.write('Начинаем парсинг данных...')
            run_parsing_async()
            st.success('Данные успешно сохранены в базу данных!')
    else:
        st.markdown(f'''Данные актуальны 👍  
                    Последняя дата в базе данных: {last_date_db}  
                    Последняя дата на сайте: {last_date_site}
                    ''')

st.subheader('Поиск по имени')        
# Функция для поиска по базе данных
def search_database(query):
    db_url = 'sqlite:///mydatabase.db'
    engine = create_engine(db_url)
    with engine.connect() as connection:
        # Используем SQL для поиска
        query_like = f"%{query}%"
        sql_query = text("SELECT * FROM runners WHERE name LIKE :query")
        result = connection.execute(sql_query, {"query": query_like}).fetchall()  # Передаем параметры как словарь
    return result

# Поле для поиска
search_query = st.text_input("Поиск по имени бегуна:")

# Обработка поискового запроса
if search_query:
    search_results = search_database(search_query)

    if search_results:
        # Преобразуем результаты в DataFrame
        df_results = pd.DataFrame(search_results)  # Без указания имен колонок
        st.dataframe(df_results)  # Отображаем результаты в виде таблицы
    else:
        st.write("Нет результатов по вашему запросу.")