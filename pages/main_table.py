import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

st.set_page_config(layout='wide', initial_sidebar_state='collapsed')

st.header('База участников 5Верст в Петергофе')

engine = create_engine('sqlite:///mydatabase.db')
querie = '''
SELECT 
    COALESCE(r.id, o.id) AS id,
    COALESCE(r.first_name, o.first_name) AS first_name,
    COALESCE(r.last_name, o.last_name) AS last_name,
    COALESCE(r.min_time, '00:00:00') AS min_time,
    COALESCE(r.number_of_runs, 0) AS number_of_runs,
    COALESCE(r.number_of_runs_in_peterhof, 0) AS number_of_runs_in_peterhof,
    COALESCE(o.number_of_helps, 0) AS number_of_helps,
    COALESCE(o.number_of_helps_in_peterhof, 0) AS number_of_helps_in_peterhof,
    COALESCE(r.profile_link, o.profile_link) AS profile_link
FROM 
    (SELECT  
        participant_id AS id,
        SUBSTR(name, 1, INSTR(name, ' ') - 1) AS first_name,
        SUBSTR(name, INSTR(name, ' ') + 1) AS last_name,
        MIN(TIME(time)) AS min_time,
        COUNT(DISTINCT run_date) AS number_of_runs,
        SUM(CASE WHEN run = 'Петергоф Александрийский' THEN 1 ELSE 0 END) AS number_of_runs_in_peterhof,
        profile_link
    FROM runners 
    GROUP BY participant_id, name, profile_link
    ) AS r
LEFT JOIN 
    (SELECT  
        participant_id AS id,
        SUBSTR(name, 1, INSTR(name, ' ') - 1) AS first_name,
        SUBSTR(name, INSTR(name, ' ') + 1) AS last_name,
        COUNT(DISTINCT run_date) AS number_of_helps,
        COUNT(DISTINCT CASE WHEN run = 'Петергоф Александрийский' THEN run_date END) AS number_of_helps_in_peterhof,
        profile_link
    FROM organizers 
    GROUP BY participant_id, name, profile_link
    ) AS o
ON r.id = o.id

UNION ALL

SELECT 
    o.id,
    o.first_name,
    o.last_name,
    '00:00:00' AS min_time,
    0 AS number_of_runs,
    0 AS number_of_runs_in_peterhof,
    o.number_of_helps,
    o.number_of_helps_in_peterhof,
    o.profile_link
FROM 
    (SELECT  
        participant_id AS id,
        SUBSTR(name, 1, INSTR(name, ' ') - 1) AS first_name,
        SUBSTR(name, INSTR(name, ' ') + 1) AS last_name,
        COUNT(DISTINCT run_date) AS number_of_helps,
        COUNT(DISTINCT CASE WHEN run = 'Петергоф Александрийский' THEN run_date END) AS number_of_helps_in_peterhof,
        profile_link
    FROM organizers 
    GROUP BY participant_id, name, profile_link
    ) AS o
LEFT JOIN 
    (SELECT  
        participant_id AS id
    FROM runners 
    GROUP BY participant_id
    ) AS r
ON r.id = o.id
WHERE r.id IS NULL
'''
df = pd.read_sql(querie, con=engine)

# Отображаем таблицу 
st.data_editor(
    df,
    column_config={
        'profile_link': st.column_config.LinkColumn(label="id 5Вёрст", display_text=r"([0-9]*)$", width='small'),
        'last_name': st.column_config.Column(label="Фамилия", width='medium'),     
        'first_name': st.column_config.Column(label="Имя", width='medium'),
        'min_time': st.column_config.Column(label="Рекорд", width='small'),
        'number_of_runs': st.column_config.Column(label="# финишей", width='medium'),
        'number_of_helps': st.column_config.Column(label="# волонтерств", width='medium'),
    },
    hide_index=True
)