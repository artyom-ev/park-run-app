import streamlit as st
from menu import menu
import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client, Client

SUPABASE_URL = st.secrets['SUPABASE_URL']
SUPABASE_KEY = st.secrets['SUPABASE_KEY']
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


st.set_page_config(page_title='Duck Run', 
                    page_icon='🌳',
                    layout='wide', 
                    initial_sidebar_state='collapsed')
menu()

image_path = 'logo.jpg'
st.image(image_path, caption='')


st.divider()
col1, col2 = st.columns(2)

with col1:
    st.subheader('Список страниц:')
    
    st.markdown('''
    - [База участников](main_table)
    - [Клубы и рекорды](records)
    ''')

with col2:
    st.subheader('Актуальность данных:')
    
    last_date_response = (supabase.table('petergof_participants').select('date').execute())
    last_date_df = pd.DataFrame(last_date_response.data)
    last_date_df['date'] = pd.to_datetime(last_date_df['date'], dayfirst=True)
    last_date_db = last_date_df['date'].max().strftime('%d-%m-%Y')
    st.write(f'Последняя дата в базе данных: {last_date_db}')
    
    def get_last_date_from_site():
        url = 'https://5verst.ru/petergofaleksandriysky/results/all/'
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        cell_date = soup.find_all('table')[0].find_all('tr')[1].find_all('td')[1]
        last_date = cell_date.text.strip()
        link = cell_date.find('a')['href']
        last_date_site = datetime.strptime(last_date, '%d.%m.%Y').strftime('%d-%m-%Y')
        return last_date_site, link
    last_date_site, last_date_link = get_last_date_from_site()
    
    st.markdown(f'Последняя дата на сайте: [{last_date_site}]({last_date_link})')
    if last_date_db == last_date_site:
        st.write('Данные актуальны 👍')
    else: 
        st.write('Нужно обновить данные 🕵️')


st.divider()
st.subheader('Поиск по волонтерской роли:')

def fetch_unique_roles():
    response = supabase.table('petergof_volunteers').select('volunteer_role').execute()
    roles = set([record['volunteer_role'] for record in response.data])
    return sorted(roles)

def fetch_volunteers_data():
    response = supabase.table('petergof_volunteers').select('date', 'name', 'profile_link', 'volunteer_role').execute()
    return response.data

unique_roles = fetch_unique_roles()

selected_role = st.selectbox(
    'Поиск по волонтерской роли',
    options=unique_roles,
    index=None,
    placeholder='Выберите роль',
    label_visibility='collapsed'
    )

if selected_role:
    df = pd.DataFrame(fetch_volunteers_data())
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    grouped = df.groupby(['name', 'profile_link', 'volunteer_role']).agg({'date': [('number', 'count'),
                                                                        ('last_date_of_role', 'max')]})
    grouped.columns = [col for _, col in grouped.columns]
    grouped = grouped.reset_index()
    grouped = grouped.sort_values(by='last_date_of_role', ascending=False)
    grouped['last_date_of_role'] = grouped['last_date_of_role'].dt.strftime('%d-%m-%Y')
    grouped = grouped[grouped['volunteer_role'] == selected_role]
    with st.container():
        st.data_editor(
            grouped[['profile_link', 'name', 'volunteer_role', 'number', 'last_date_of_role']],
            column_config={
                'profile_link': st.column_config.LinkColumn(label='id 5Вёрст', display_text=r'([0-9]*)$', width='100px'),
                'name': st.column_config.Column(label='Имя', width='120px'), 
                'volunteer_role': st.column_config.Column(label='Роль', width='medium'),
                'number': st.column_config.Column(label='#', width='small'),
                'last_date_of_role': st.column_config.Column(label='Последняя дата', width='medium'),
            },
            hide_index=True,
            key='roles'
        )


st.divider()
st.subheader('Поиск участника по имени:')

partial_name = st.text_input(
    'Введите имя или часть имени',
    placeholder='Введите имя',
    label_visibility='collapsed'
)

def fetch_search_data(partial_name):
    response = supabase.table('petergof_summary').select('*').ilike('name', f'%{partial_name}%').execute()
    return response.data

if partial_name:
    data = fetch_search_data(partial_name)
    if data:
        df_results = pd.DataFrame(data)
        with st.container():
            st.data_editor(
                df_results[['profile_link', 'name', 'best_time', 'n_finishes', 'n_finishes_home', 'n_volunteers', 'n_volunteers_home', 'clubs']],
                column_config={
                    'profile_link': st.column_config.LinkColumn(label="id 5Вёрст", display_text=r"([0-9]*)$", width='100px'),
                    'name': st.column_config.Column(label="Имя", width='120px'), 
                    'best_time': st.column_config.Column(label="Лучшее время", width='100px'),
                    'n_finishes': st.column_config.Column(label="# финишей", width='100px'),
                    'n_finishes_home': st.column_config.Column(label="# финишей в Петергофе", width='150px'),
                    'n_volunteers': st.column_config.Column(label="# волонтерств", width='120px'),
                    'n_volunteers_home': st.column_config.Column(label="# волонтерств в Петергофе", width='150px'),
                    'clubs': st.column_config.Column(label="Клубы", width='large'),
                },
                hide_index=True,
                key="custom_table"
            )
    else:
        st.write(f"Нет данных для '{partial_name}'.")