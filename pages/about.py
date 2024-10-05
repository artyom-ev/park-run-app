import os
import streamlit as st
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd  

# Конфигурация страницы
st.set_page_config(page_title='PARK🌳RUN', page_icon=':running:')

# Путь к изображению
image_path = 'logo.jpg'  # Замените на путь к вашему изображению

# Вставка изображения
st.image(image_path, caption='', width=250)

# Скрытие футера
hide_streamlit_style = """
    <style>
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

# Функция для парсинга сайта с прогрессом
def parse_website():
    # URL с таблицей результатов
    url = 'https://5verst.ru/results/latest/'

    # Получаем HTML-страницу
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Находим таблицу с результатами
    table = soup.find('table')  

    # Список для хранения данных
    starts_latest = []

    # Проходим по каждой строке таблицы
    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        if len(cells) > 0:
            # Извлекаем данные из строк таблицы
            number_cell = cells[0]
            run = number_cell.text.strip().split(' #')[0]  # Старт
            link = number_cell.find('a')['href'] if number_cell.find('a') else None
            date = cells[1].text.strip()  # Дата забега
            finishers = cells[2].text.strip()  # Число финишёров
            volunteers = cells[3].text.strip()  # Число волонтёров
            avg_time = cells[4].text.strip()  # Среднее время
            best_female_time = cells[5].text.strip()  # Лучшее время "Ж"
            best_male_time = cells[6].text.strip()  # Лучшее время "М"

            # Добавляем строку с данными
            starts_latest.append([run, date, link, finishers, volunteers, avg_time, best_female_time, best_male_time])

    # Создаем DataFrame из данных
    starts_latest = pd.DataFrame(starts_latest, columns=['run', 'run_date', 'run_link', 'finishers', 'volunteers', 'avg_time', 'best_female_time', 'best_male_time'])

    # Список забегов, с которыми будем работать
    target_runs = ['Петергоф Александрийский']

    # Фильтруем DataFrame по списку забегов
    filtered_starts_latest = starts_latest[starts_latest['run'].isin(target_runs)]

    # Создаем новый список для хранения данных по локациям
    starts_data = []

    # Проходим по отфильтрованным данным
    for index, row in filtered_starts_latest.iterrows():
        # Переходим на страницу забега
        run_url = row['run_link']
        if run_url:
            run_response = requests.get(run_url)
            run_soup = BeautifulSoup(run_response.text, 'html.parser')

            # Парсим таблицу результатов забега
            run_table = run_soup.find('table')
            
            run_data = []

            # Проходим по каждой строке таблицы
            for run_row in run_table.find_all('tr')[1:]:
                run_cells = run_row.find_all('td')
                if len(run_cells) > 0:
                    number = run_cells[0].text.strip()  # Номер забега
                    date_cell = run_cells[1]
                    link = date_cell.find('a')['href'] if date_cell.find('a') else None
                    date = date_cell.text.strip()  # Дата забега

                    finishers = run_cells[2].text.strip()  # Число финишёров
                    volunteers = run_cells[3].text.strip()  # Число волонтёров
                    avg_time = run_cells[4].text.strip()  # Среднее время
                    best_female_time = run_cells[5].text.strip()  # Лучшее время "Ж"
                    best_male_time = run_cells[6].text.strip()  # Лучшее время "М"

                    # Добавляем строку с данными
                    run_data.append([number, date, link, finishers, volunteers, avg_time, best_female_time, best_male_time])

            # Добавляем название забега из списка и данные по каждому забегу
            for item in run_data:
                starts_data.append([row['run']] + item)  # Название забега + остальные данные

    # Создаем итоговый DataFrame
    starts_data = pd.DataFrame(starts_data, columns=['run', 'run_number', 'run_date', 'run_link', 'finishers', 'volunteers', 'avg_time', 'best_female_time', 'best_male_time'])

    orgs_data = []
    runners_data = []

    # Проходим по отфильтрованным данным
    for index, df_row in starts_data.iterrows():  
        # Переходим на страницу забега
        run_url = df_row['run_link']
        if run_url:
            # Выполняем GET-запрос для получения страницы
            response = requests.get(run_url)
            
            # Проверяем, что запрос успешен
            if response.status_code != 200:
                print(f"Ошибка при загрузке страницы: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            # Находим все таблицы на странице
            all_tables = soup.find_all('table')

            # Проверяем, что на странице есть как минимум две таблицы
            if len(all_tables) < 2:
                print('Не удалось найти вторую таблицу на странице')
                continue
            
            # Находим таблицу с результатами участников
            run_table = all_tables[0]
            if run_table:
                # Проходим по каждой строке таблицы с участниками
                for run_row in run_table.find_all('tr')[1:]:
                    run_cells = run_row.find_all('td')
                    if len(run_cells) >= 4:
                        # Парсим данные о каждом участнике
                        position = run_cells[0].get_text(strip=True) if run_cells[0] else '—'

                        # Имя и ссылка на профиль
                        name_tag = run_cells[1].find('a')
                        name = name_tag.get_text(strip=True) if name_tag else '—'
                        profile_link = name_tag['href'] if name_tag else '—'

                        # Уникальный номер участника
                        participant_id = profile_link.split('/')[-1] if profile_link != '—' else '—'

                        # Количество финишей и волонтёрств
                        stats_div = run_cells[1].find('div', class_='user-stat')
                        finishes = '—'
                        volunteers = '—'
                        if stats_div:
                            stats_spans = stats_div.find_all('span')
                            finishes = stats_spans[0].get_text(strip=True).split(' ')[0] if len(stats_spans) > 0 else '—'
                            volunteers = stats_spans[1].get_text(strip=True).split(' ')[0] if len(stats_spans) > 1 else '—'

                        # Клубы
                        club_tags = run_cells[1].find_all('span', class_='club-icon')
                        clubs = ', '.join([club['title'] for club in club_tags]) if club_tags else '—'

                        # Возрастная группа и Age Grade
                        age_group = run_cells[2].get_text(strip=True).split(' ')[0] if run_cells[2] else '—'
                        age_grade_tag = run_cells[2].find('div', class_='age_grade')
                        age_grade = age_grade_tag.get_text(strip=True) if age_grade_tag else '—'

                        # Время и достижения
                        time = run_cells[3].get_text(strip=True) if run_cells[3] else '—'
                        achievements = []
                        achievements_div = run_cells[3].find('div', class_='table-achievments')
                        if achievements_div:
                            achievement_icons = achievements_div.find_all('span', class_='results_icon')
                            for icon in achievement_icons:
                                achievements.append(icon['title'])  # Описание достижения

                        # Добавляем строку с данными в итоговый список
                        runners_data.append([
                            df_row['run'], df_row['run_number'], df_row['run_date'], df_row['run_link'], df_row['finishers'], 
                            df_row['volunteers'], df_row['avg_time'], df_row['best_female_time'], df_row['best_male_time'], 
                            position, name, profile_link, participant_id, clubs, finishes, volunteers, age_group, age_grade, time, ', '.join(achievements)
                        ])

            # Получаем вторую таблицу 
            orgs_table = all_tables[1]

            # Находим все строки таблицы
            table_rows = orgs_table.find_all('tr')

            # Проходим по каждой строке таблицы
            for html_row in table_rows:  
                columns = html_row.find_all('td')

                if len(columns) > 1:  # Проверяем, что в строке как минимум 2 колонки
                    # Первая колонка: имя, ссылка на профиль, количество финишей и волонтёрств
                    name_tag = columns[0].find('a')
                    name = name_tag.get_text(strip=True) if name_tag else '—'
                    profile_link = name_tag['href'] if name_tag else '—'
                    
                    # Извлекаем уникальный номер участника из ссылки
                    participant_id = profile_link.split('/')[-1] if profile_link else '—'

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

                    # Добавляем собранные данные в список
                    orgs_data.append([df_row['run'], df_row['run_number'], df_row['run_date'], df_row['run_link'], df_row['finishers'], 
                                df_row['volunteers'], df_row['avg_time'], df_row['best_female_time'], df_row['best_male_time'],
                                    name, profile_link, participant_id, finishes, volunteers, clubs, volunteer_role, first_volunteer_info
                                    ])
    return orgs_data, runners_data 

# Функция для сохранения данных в базу данных
def save_to_database(df_orgs, df_runners, db_url='sqlite:///mydatabase.db'):
    # Создаем подключение к базе данных
    engine = create_engine(db_url)

    # Сохраняем данные организаторов в таблицу 'organizers'
    df_orgs.to_sql('organizers', con=engine, if_exists='replace', index=False)

    # Сохраняем данные бегунов в таблицу 'runners'
    df_runners.to_sql('runners', con=engine, if_exists='replace', index=False)

#Парсинг
def run_parsing():
    # Парсим данные (функция должна возвращать два объекта)
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

    # Сохраняем данные в базу данных
    save_to_database(df_orgs, df_runners)


last_date_site = get_last_date_from_site()
last_date_db = get_last_date_from_db()
    
st.subheader('Актуальность данных')  
# Проверяем, если last_date_db не пустая
if last_date_db is None:
    st.write('Данных в базе нет!')
    if st.button('Обновить данные'):
        st.write('Начинаем парсинг данных...')
        run_parsing()
        st.success('Данные успешно сохранены в базу данных!')
else:
    # Сравнение дат
    if last_date_db != last_date_site:
        st.write(f'Данные устарели. Дата в базе: {last_date_db}, дата на сайте: {last_date_site}.')
        
        if st.button('Обновить данные'):
            st.write('Начинаем парсинг данных...')
            run_parsing()
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