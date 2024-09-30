import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

st.set_page_config(layout='wide')

st.title('Данные по пробегам')

engine = create_engine('sqlite:///mydatabase.db')
querie = '''
SELECT   run, run_number, run_date, finisher, volunteer, avg_time, best_female_time, best_male_time, position, 
name, profile_link, participant_id, clubs, finishes, volunteers, age_group, age_grade, time, achievements
FROM runners
'''
df = pd.read_sql(querie, con=engine)

# Отображаем таблицу в широком режиме
st.dataframe(df, use_container_width=True)