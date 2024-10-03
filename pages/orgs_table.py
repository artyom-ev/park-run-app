import pandas as pd
import streamlit as st

st.set_page_config(layout='wide', initial_sidebar_state='collapsed')

st.title('Данные по пробегам и организаторам')

# Чтение данных из CSV файлов
df_organizers = pd.read_csv('organizers.csv')
df_organizers['participant_id'] = df_organizers['participant_id'].astype(str)

# Используем st.data_editor для отображения таблицы
st.data_editor(
    df_organizers,
    column_config={
        'profile_link': st.column_config.LinkColumn(),
        'run_link': st.column_config.LinkColumn(),
    },
    hide_index=True
)