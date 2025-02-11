import streamlit as st
import pandas as pd
from menu import menu
from supabase import create_client, Client

SUPABASE_URL = st.secrets['SUPABASE_URL']
SUPABASE_KEY = st.secrets['SUPABASE_KEY']
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

st.set_page_config(page_title='Records', 
                    page_icon='🏆',
                    layout='wide', 
                    initial_sidebar_state='collapsed')
menu()

st.header('Таблицы по рекордсменам, новичкам и вступившим в клубы 10/25/50/100')


st.subheader('Рекорды')
records = (supabase.table('view_records').select('*').execute())
records = pd.DataFrame(records.data)
st.data_editor(
    records,
    column_config={
        'profile_link': st.column_config.LinkColumn(label='id 5Вёрст', display_text=r'([0-9]*)$', width=''),
        'name': st.column_config.Column(label='Участник', width='medium'), 
        'time': st.column_config.Column(label='Время', width=''), 
        'position': st.column_config.Column(label='Позиция', width='')
    },
    hide_index=True
)


st.subheader('Первый финиш на 5 верст')
first_finish = (supabase.table('view_first_finish').select('*').execute())
first_finish = pd.DataFrame(first_finish.data)
st.data_editor(
    first_finish,
    column_config={
        'profile_link': st.column_config.LinkColumn(label='id 5Вёрст', display_text=r'([0-9]*)$', width=''),
        'name': st.column_config.Column(label='Участник', width='medium'), 
        'time': st.column_config.Column(label='Время', width=''),
        'position': st.column_config.Column(label='Позиция', width='')
    },
    hide_index=True
)


st.subheader('Первый финиш в Петергофе')
first_finish_petergof = (supabase.table('view_first_finish_petergof').select('*').execute())
first_finish_petergof = pd.DataFrame(first_finish_petergof.data)
st.data_editor(
    first_finish_petergof,
    column_config={
        'profile_link': st.column_config.LinkColumn(label='id 5Вёрст', display_text=r'([0-9]*)$', width=''),
        'name': st.column_config.Column(label='Участник', width='medium'), 
        'time': st.column_config.Column(label='Время', width=''),
        'position': st.column_config.Column(label='Позиция', width=''), 
        'n_finishes': st.column_config.Column(label='# финишей', width='medium')
    },
    hide_index=True
)


st.subheader('Первое волонтерство на 5 верст')
first_volunteer = (supabase.table('view_first_volunteer').select('*').execute())
first_volunteer = pd.DataFrame(first_volunteer.data)
st.data_editor(
    first_volunteer,
    column_config={
        'profile_link': st.column_config.LinkColumn(label='id 5Вёрст', display_text=r'([0-9]*)$', width=''),
        'name': st.column_config.Column(label='Участник', width='medium'), 
        'time': st.column_config.Column(label='Время', width=''),
        'position': st.column_config.Column(label='Позиция', width='')
    },
    hide_index=True
)


st.subheader('Первое волонтерство в Петергофе')
first_volunteer_petergof = (supabase.table('view_first_volunteer_petergof').select('*').execute())
first_volunteer_petergof = pd.DataFrame(first_volunteer_petergof.data)
st.data_editor(
    first_volunteer_petergof,
    column_config={
        'profile_link': st.column_config.LinkColumn(label='id 5Вёрст', display_text=r'([0-9]*)$', width=''),
        'name': st.column_config.Column(label='Участник', width='medium'), 
        'volunteers': st.column_config.Column(label='# волонтерств', width=''),
        'time': st.column_config.Column(label='Время', width=''),
        'position': st.column_config.Column(label='Позиция', width='')
    },
    hide_index=True
)


st.subheader('Вступившие в клубы пробегов')
view_run_clubs = (supabase.table('view_run_clubs').select('*').execute())
view_run_clubs = pd.DataFrame(view_run_clubs.data)
st.data_editor(
    view_run_clubs,
    hide_index=True
)

st.subheader('Вступившие в клубы волонтёрств')
view_help_clubs = (supabase.table('view_help_clubs').select('*').execute())
view_help_clubs = pd.DataFrame(view_help_clubs.data)
st.data_editor(
    view_help_clubs,
    hide_index=True
)

# st.subheader('Вторая суббота в Петергофе')