import pandas as pd
import streamlit as st

st.set_page_config(layout='wide', initial_sidebar_state='collapsed')

st.title('Данные по пробегам и участникам')

# Чтение данных из CSV файлов
df_runners = pd.read_csv('runners.csv')

# Используем st.data_editor для отображения таблицы
st.data_editor(
    df_runners,
    column_config={
        'profile_link': st.column_config.LinkColumn(),
        'run_link': st.column_config.LinkColumn(),
    },
    hide_index=True
)