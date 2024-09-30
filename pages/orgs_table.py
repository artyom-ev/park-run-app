import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

st.set_page_config(layout='wide')

st.title('Данные по организаторам')

engine = create_engine('sqlite:///mydatabase.db')
querie = '''
SELECT run, run_number, run_date, finisher, name, profile_link, participant_id, finishes, volunteers, clubs, volunteer_role, first_volunteer_info
FROM organizers
'''
df = pd.read_sql(querie, con=engine)

# Отображаем таблицу в широком режиме
st.dataframe(df, use_container_width=True)