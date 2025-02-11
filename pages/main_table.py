import streamlit as st
from menu import menu
import pandas as pd
from supabase import create_client, Client

SUPABASE_URL = st.secrets['SUPABASE_URL']
SUPABASE_KEY = st.secrets['SUPABASE_KEY']
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

st.set_page_config(page_title='Main Table', 
                    page_icon='📊',
                    layout='wide', 
                    initial_sidebar_state='collapsed')
menu()


st.header('База участников 5Верст в Петергофе')

home_run_response = (
    supabase.table('petergof_summary')
    .select('*')
    .execute()
)
home_run_df = pd.DataFrame(home_run_response.data)

st.markdown(f'''
            Уникальных участников в таблице {len(home_run_df)}  
            ''')

table_css = '''
    <style>
    .data-editor-container {
        width: 800px;  /* Ширина всей таблицы */
        margin: 0 auto;  /* Центрирование */
    }
    </style>
'''

st.markdown(table_css, unsafe_allow_html=True)

with st.container():
    st.data_editor(
        home_run_df[['profile_link', 'name', 'sex', 'best_time', 'n_finishes', 'n_finishes_home',
                    'r_latest_date', 'n_volunteers', 'n_volunteers_home', 'v_latest_date', 'clubs']],
        column_config={
            'profile_link': st.column_config.LinkColumn(label='id 5Вёрст', display_text=r'([0-9]*)$', width='100px'),
            'name': st.column_config.Column(label='Участник', width='large'), 
            'sex': st.column_config.Column(label='Пол', width='10px'), 
            'best_time': st.column_config.Column(label='Лучшее время', width='100px'),
            'n_finishes': st.column_config.Column(label='# финишей', width='100px'),
            'n_finishes_home': st.column_config.Column(label='# финишей в Петергофе', width='150px'),
            'r_latest_date': st.column_config.Column(label='Дата последнего финиша', width='150px'),
            'n_volunteers': st.column_config.Column(label='# волонтерств', width='120px'),
            'n_volunteers_home': st.column_config.Column(label='# волонтерств в Петергофе', width='150px'),
            'v_latest_date': st.column_config.Column(label='Дата последнего волонтёрства', width='150px'),
            'clubs': st.column_config.Column(label='Клубы', width='large'),
        },
        hide_index=True,
        key='custom_table'
    )