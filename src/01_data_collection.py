import json
import time
import asyncio
import aiohttp
import logging
from pathlib import Path
from tqdm import tqdm
from datetime import datetime
from bs4 import BeautifulSoup

# Определяем константы
BASE_DIR = Path(__file__).resolve().parent.parent  
DATA_DIR = BASE_DIR / 'data'  
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR = BASE_DIR / 'data' / 'logs'
LOGS_DIR.mkdir(parents=True, exist_ok=True)
TIMESTAMP = datetime.now().strftime('%Y-%m-%d_%H-%M')
LOG_FILE = LOGS_DIR / f'logs_from_{TIMESTAMP}.log'

# Настройки логирования
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Настройки парсинга
EVENTS_URL = 'https://5verst.ru/events/'
MAX_CONCURRENT_REQUESTS = 25  
TIMEOUT_SECONDS = 60

async def fetch(session, url):
    """Асинхронно получает HTML-код страницы."""
    async with session.get(url, timeout=TIMEOUT_SECONDS) as response:
        return await response.text()

async def fetch_events(session):
    """Собирает список событий с главной страницы."""
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
    """Парсит детали забега."""
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
    """Парсит участников и волонтеров забега."""
    html = await fetch(session, run_link)
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')
    
    if not tables:
        logger.error(f'Ошибка: на странице {run_link} не найдено таблиц.')
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
        logger.error(f'Ошибка: на странице {run_link} отсутствует таблица волонтёров.')
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

def save_data_to_json(data: dict, filepath: str):
    with open(filepath, 'w', encoding='utf8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

async def main():
    start_time = time.time()  
    logger.info('Начало выполнения...')
    """Основная функция для сбора и парсинга данных."""
    async with aiohttp.ClientSession() as session:
        # Собираем список по всем локациям 
        logger.info('Сбор данных по всем локациям...')
        events = await fetch_events(session)
        logger.info(f'Найдено всего {len(events)} локаций.')

        # Парсим детали забегов для каждого события
        logger.info('Парсинг информации по пробегам...')
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        tasks = [parse_run_details(session, event, semaphore) for event in events]
        results = await asyncio.gather(*tasks)

        # Объединяем результаты
        events = [detail for sublist in results for detail in sublist]
        # events = [event for event in events if event['run'].startswith('Петергоф Александрийский') and not event['run'].endswith('0')] # фильтр на хоумранс
        logger.info(f'{len(events)} - общее количество проведенных мероприятий')

        # Сохраняем 
        events_file_path = DATA_DIR / f'events.json'
        save_data_to_json(events, events_file_path)
        logger.info(f'Данные по мероприятиям сохранены в файл {events_file_path}')

        # Создаем списки для участников и волонтеров
        participants_data = []
        volunteers_data = []
        
        # Собираем информацию об участниках и волонтерах для каждого забега
        for run in tqdm(events, desc='Обработка забегов'):
            participants, volunteers = await parse_participants_and_volunteers(session, run['link'])
            
            # Добавляем информацию о забеге к каждому участнику
            for participant in participants:
                participant_data = {
                    **run,  
                    **participant  
                }
                participants_data.append(participant_data)
            
            # Добавляем информацию о забеге к каждому волонтеру
            for volunteer in volunteers:
                volunteer_data = {
                    **run,  
                    **volunteer  
                }
                volunteers_data.append(volunteer_data)
                
        logger.info(f'Общее число финишей - {len(participants_data)} и общее число волонтёрств - {len(volunteers_data)}')

        # Сохраняем данные 
        participants__file_path = DATA_DIR / f'participants.json'
        volunteers_file_path = DATA_DIR / f'volunteers.json'
        save_data_to_json(participants_data, participants__file_path)
        save_data_to_json(volunteers_data, volunteers_file_path)
        logger.info(f'Данные по участникам и волонтёрам сохранены в файлы {participants__file_path} и {volunteers_file_path}')

    end_time = time.time()  
    elapsed_time = end_time - start_time  
    logger.info(f'Время выполнения сбора данных: {elapsed_time:.2f} секунд')

# Запуск асинхронного кода
if __name__ == '__main__':
    asyncio.run(main())