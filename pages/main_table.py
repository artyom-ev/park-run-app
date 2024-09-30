import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

st.set_page_config(layout='wide')

st.title('Основная таблица по результатам и волонтёрствам участников')

engine = create_engine('sqlite:///mydatabase.db')
querie = '''
-- Объединение бегунов и организаторов с учетом profile_link
SELECT 
    COALESCE(r.id, o.id) AS id,
    COALESCE(r.first_name, o.first_name) AS first_name,
    COALESCE(r.last_name, o.last_name) AS last_name,
    COALESCE(min_time, '00:00:00') AS min_time,
    COALESCE(number_of_runs, 0) AS number_of_runs,
    COALESCE(number_of_runs_in_peterhof, 0) AS number_of_runs_in_peterhof,
    COALESCE(number_of_helps, 0) AS number_of_helps,
    COALESCE(number_of_helps_in_peterhof, 0) AS number_of_helps_in_peterhof,
    COALESCE(r.profile_link, o.profile_link) AS profile_link  -- Выбираем profile_link из обеих таблиц
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
        COUNT(DISTINCT CASE WHEN run = 'Петергоф Александрийский' THEN run_date END) AS number_of_helps_in_peterhof,  -- Уникальные даты для Петергофа
        profile_link
    FROM organizers 
    GROUP BY participant_id, name, profile_link
    ) AS o
ON r.id = o.id

UNION

-- Для случаев, когда человек был только организатором, но не бегуном
SELECT 
    COALESCE(r.id, o.id) AS id,
    COALESCE(r.first_name, o.first_name) AS first_name,
    COALESCE(r.last_name, o.last_name) AS last_name,
    COALESCE(min_time, '00:00:00') AS min_time,
    COALESCE(number_of_runs, 0) AS number_of_runs,
    COALESCE(number_of_runs_in_peterhof, 0) AS number_of_runs_in_peterhof,
    COALESCE(number_of_helps, 0) AS number_of_helps,
    COALESCE(number_of_helps_in_peterhof, 0) AS number_of_helps_in_peterhof,
    COALESCE(r.profile_link, o.profile_link) AS profile_link  -- Выбираем profile_link из обеих таблиц
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
RIGHT JOIN 
    (SELECT  
        participant_id AS id,
        SUBSTR(name, 1, INSTR(name, ' ') - 1) AS first_name,
        SUBSTR(name, INSTR(name, ' ') + 1) AS last_name,
        COUNT(DISTINCT run_date) AS number_of_helps,
        COUNT(DISTINCT CASE WHEN run = 'Петергоф Александрийский' THEN run_date END) AS number_of_helps_in_peterhof,  -- Уникальные даты для Петергофа
        profile_link
    FROM organizers 
    GROUP BY participant_id, name, profile_link
    ) AS o
ON r.id = o.id
WHERE r.id IS NULL
'''
df = pd.read_sql(querie, con=engine)

# Отображаем таблицу в широком режиме
st.dataframe(df, use_container_width=True)