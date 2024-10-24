import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
from menu import menu

# Установка конфигурации страницы
st.set_page_config(layout='wide', initial_sidebar_state='collapsed')

menu()

engine = create_engine('sqlite:///mydatabase.db')

# Заголовок
st.title('Таблицы по рекордсменам, новичкам и вступившим в клубы 10/25/50/100')

# st.header('Таблица для сверки результатов')

# querie = '''
# SELECT 
#     profile_link,
#     name,
#     finishes,
#     volunteers,
#     achievements
# FROM runners
# WHERE (
#     finishes IN ('10 финишей', '25 финишей', '50 финишей', '100 финишей')
#     OR volunteers IN ('10 волонтёрств', '25 волонтёрств', '50 волонтёрств', '100 волонтёрств')
#     OR (achievements IS NOT NULL AND TRIM(achievements) != '')
# )
# AND run_date = (
#     SELECT MAX(run_date)
#     FROM runners
# );
# '''

# df = pd.read_sql(querie, con=engine)

# # Отображаем таблицу
# st.data_editor(
#     df,
#     column_config={
#         'profile_link': st.column_config.LinkColumn(label="id 5Вёрст", display_text=r"([0-9]*)$", width='medium'),
#         'name': st.column_config.Column(label="Участник", width='large'), 
#         'finishes': st.column_config.Column(label="# финишей", width='medium'),
#         'volunteers': st.column_config.Column(label="# волонтерств", width='medium'),
#         'achievements': st.column_config.Column(label="Достижения", width='large'),
#     },
#     hide_index=True
# )

st.header('Рекорды')

querie = '''
SELECT 
    profile_link,
    name,
    time
    --finishes,
    --volunteers,
    --achievements
FROM runners
WHERE (
achievements LIKE '%Личный рекорд!%' 
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
        'profile_link': st.column_config.LinkColumn(label="id 5Вёрст", display_text=r"([0-9]*)$", width='medium'),
        'name': st.column_config.Column(label="Участник", width='large'), 
        'time': st.column_config.Column(label="Время", width='large'), 
        'finishes': st.column_config.Column(label="# финишей", width='medium'),
        'volunteers': st.column_config.Column(label="# волонтерств", width='medium'),
        'achievements': st.column_config.Column(label="Достижения", width='large'),
    },
    hide_index=True
)

# st.write(df['name'].unique())

st.header('Первый финиш на 5 верст')

querie = '''
SELECT 
    profile_link,
    name
    --finishes,
    --volunteers,
    --achievements
FROM runners
WHERE (
achievements LIKE '%Первый финиш на 5 вёрст%'
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
        'profile_link': st.column_config.LinkColumn(label="id 5Вёрст", display_text=r"([0-9]*)$", width='medium'),
        'name': st.column_config.Column(label="Участник", width='large'), 
        'finishes': st.column_config.Column(label="# финишей", width='medium'),
        'volunteers': st.column_config.Column(label="# волонтерств", width='medium'),
        'achievements': st.column_config.Column(label="Достижения", width='large'),
    },
    hide_index=True
)

st.header('Первый финиш в Петергофе')

querie = '''
SELECT 
    profile_link,
    name
    --finishes,
    --volunteers,
    --achievements
FROM runners
WHERE (
achievements LIKE '%Первый финиш на Петергоф Александрийский%'
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
        'profile_link': st.column_config.LinkColumn(label="id 5Вёрст", display_text=r"([0-9]*)$", width='medium'),
        'name': st.column_config.Column(label="Участник", width='large'), 
        'finishes': st.column_config.Column(label="# финишей", width='medium'),
        'volunteers': st.column_config.Column(label="# волонтерств", width='medium'),
        'achievements': st.column_config.Column(label="Достижения", width='large'),
    },
    hide_index=True
)

st.header('Втупившие в клубы пробегов')

querie = '''
WITH ranked_runs AS (
  SELECT 
    name,
    run_date,
    finishes,
    ROW_NUMBER() OVER (PARTITION BY name ORDER BY run_date DESC) AS run_rank
  FROM runners
)
SELECT 
    p.profile_link,
    p.name,
    p.finishes,
    rr1.run_date AS last_date,
    rr1.finishes AS last_finishes,
    rr2.run_date AS second_to_last_date,
    rr2.finishes AS second_to_last_finishes
FROM (
    SELECT 
        profile_link,
        name,
        finishes
    FROM runners
    WHERE 
        finishes IN ('10 финишей', '25 финишей', '50 финишей', '100 финишей')
        AND run_date = (SELECT MAX(run_date) FROM runners)
) p
LEFT JOIN ranked_runs rr1
    ON p.name = rr1.name AND rr1.run_rank = 1
LEFT JOIN ranked_runs rr2
    ON p.name = rr2.name AND rr2.run_rank = 2;
'''

df = pd.read_sql(querie, con=engine)

# Отображаем таблицу
st.data_editor(
    df,
    column_config={
        'profile_link': st.column_config.LinkColumn(label="id 5Вёрст", display_text=r"([0-9]*)$", width='medium'),
        'name': st.column_config.Column(label="Участник", width='large'), 
        'finishes': st.column_config.Column(label="# финишей", width='medium'),
        'volunteers': st.column_config.Column(label="# волонтерств", width='medium'),
        'achievements': st.column_config.Column(label="Достижения", width='large'),
    },
    hide_index=True
)


st.header('Втупившие в клубы волонтёрств')

querie = '''
WITH ranked_runs AS (
  SELECT 
    name,
    run_date,
    volunteers,
    ROW_NUMBER() OVER (PARTITION BY name ORDER BY run_date DESC) AS run_rank
  FROM runners
)
SELECT 
    p.profile_link,
    p.name,
    p.volunteers,
    rr1.run_date AS last_date,
    rr1.volunteers AS last_volunteers,
    rr2.run_date AS second_to_last_date,
    rr2.volunteers AS second_to_last_volunteers
FROM (
    SELECT 
        profile_link,
        name,
        volunteers
    FROM organizers
    WHERE 
        volunteers IN ('10 волонтёрств', '25 волонтёрств', '50 волонтёрств', '100 волонтёрств')
        AND run_date = (SELECT MAX(run_date) FROM organizers)
) p
LEFT JOIN ranked_runs rr1
    ON p.name = rr1.name AND rr1.run_rank = 1
LEFT JOIN ranked_runs rr2
    ON p.name = rr2.name AND rr2.run_rank = 2;
'''

df = pd.read_sql(querie, con=engine)

# Отображаем таблицу
st.data_editor(
    df,
    column_config={
        'profile_link': st.column_config.LinkColumn(label="id 5Вёрст", display_text=r"([0-9]*)$", width='medium'),
        'name': st.column_config.Column(label="Участник", width='large'), 
        'finishes': st.column_config.Column(label="# финишей", width='medium'),
        'volunteers': st.column_config.Column(label="# волонтерств", width='medium'),
        'achievements': st.column_config.Column(label="Достижения", width='large'),
    },
    hide_index=True
)

# st.header('тест')
# querie = '''
# WITH ranked_runs AS (
#   SELECT 
#     name,
#     run_date,
#     finishes,
#     ROW_NUMBER() OVER (PARTITION BY name ORDER BY run_date DESC) AS run_rank
#   FROM runners
# )
# SELECT 
#   t1.name,
#   t1.run_date AS last_date,
#   t1.finishes AS last_finishes,
#   t2.run_date AS second_to_last_date,
#   t2.finishes AS second_to_last_finishes
# FROM ranked_runs t1
# LEFT JOIN ranked_runs t2
#   ON t1.name = t2.name AND t2.run_rank = 2
# WHERE t1.run_rank = 1
# '''
# df = pd.read_sql(querie, con=engine)
# # Отображаем таблицу
# st.data_editor(
#     df,
#     hide_index=True)