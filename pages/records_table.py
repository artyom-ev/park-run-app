import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

st.set_page_config(layout='wide')

st.title('Таблица по новичкам, рекордсменам и вступившим в клубы 10/20/50/100')

engine = create_engine('sqlite:///mydatabase.db')
querie = '''
select participant_id as id,
substr(name, 1, instr(name, ' ') - 1) as first_name,
substr(name, instr(name, ' ') + 1) as last_name,
finishes,
volunteers,
achievements ,
profile_link
FROM runners
WHERE (
      finishes IN ('10 финишей', '20 финишей', '50 финишей', '100 финишей')
   OR volunteers IN ('10 волонтёрств', '20 волонтёрств', '50 волонтёрств', '100 волонтёрств')
   OR (achievements IS NOT NULL AND TRIM(achievements) != '')
)
AND STRFTIME('%Y-%m-%d', SUBSTR(run_date, 7, 4) || '-' || SUBSTR(run_date, 4, 2) || '-' || SUBSTR(run_date, 1, 2)) = (
    SELECT MAX(STRFTIME('%Y-%m-%d', SUBSTR(run_date, 7, 4) || '-' || SUBSTR(run_date, 4, 2) || '-' || SUBSTR(run_date, 1, 2)))
    FROM runners
)
'''
df = pd.read_sql(querie, con=engine)

# Отображаем таблицу в широком режиме
st.dataframe(df, use_container_width=True)