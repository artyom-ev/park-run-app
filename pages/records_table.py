import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

# Установка конфигурации страницы
st.set_page_config(layout='wide', initial_sidebar_state='collapsed')

# Заголовок
st.title('Таблицы по новичкам, рекордсменам и вступившим в клубы 10/20/50/100')

engine = create_engine('sqlite:///mydatabase.db')
querie = '''
SELECT 
    participant_id AS id,
    SUBSTR(name, 1, INSTR(name, ' ') - 1) AS first_name,
    SUBSTR(name, INSTR(name, ' ') + 1) AS last_name,
    finishes,
    volunteers,
    achievements,
    profile_link
FROM runners
WHERE (
    finishes IN ('10 финишей', '20 финишей', '50 финишей', '100 финишей')
    OR volunteers IN ('10 волонтёрств', '20 волонтёрств', '50 волонтёрств', '100 волонтёрств')
    OR (achievements IS NOT NULL AND TRIM(achievements) != '')
)
AND run_date = (
    SELECT MAX(run_date)
    FROM runners
);
'''

df = pd.read_sql(querie, con=engine)

# Отображаем таблицу 
st.data_editor(
    df,
    column_config={
        'profile_link': st.column_config.LinkColumn(),
    },
    hide_index=True
)