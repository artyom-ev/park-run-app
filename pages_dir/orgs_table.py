import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

st.set_page_config(layout='wide', initial_sidebar_state='collapsed')

st.title('Данные по пробегам и организаторам')

engine = create_engine('sqlite:///mydatabase.db')
querie = '''
SELECT run, run_number, run_date, finisher, name, profile_link, participant_id, finishes, volunteers, clubs, volunteer_role, first_volunteer_info
FROM organizers
'''
df = pd.read_sql(querie, con=engine)

df = pd.read_sql(querie, con=engine)
df['run_date'] = pd.to_datetime(df['run_date'])
df['run_date'] = df['run_date'].dt.strftime('%d.%m.%Y')

st.write(f'Всего событий {len(df)}')
st.write(f'Уникальных участников {len(df['participant_id'].unique())}')

# Отображаем таблицу 
st.data_editor(
    df,
    column_config={
        'profile_link': st.column_config.LinkColumn(),
    },
    hide_index=True
)