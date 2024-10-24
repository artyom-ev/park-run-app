import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st
from menu import menu

st.set_page_config(layout='wide', initial_sidebar_state='collapsed')

menu()

st.header('База участников 5Верст в Петергофе')

engine = create_engine('sqlite:///mydatabase.db')
    
querie = '''
SELECT profile_link, name, best_time, finishes, 
        peterhof_finishes_count, volunteers, peterhof_volunteers_count, clubs_titles
FROM users
'''
df = pd.read_sql(querie, con=engine)

st.markdown(f'''
            Уникальных участников в таблице {len(df)}  
            ''')

# # Отображаем таблицу 
# st.data_editor(
#     df,
#     column_config={
#         'profile_link': st.column_config.LinkColumn(label="id 5Вёрст", display_text=r"([0-9]*)$", width='100px'),
#         'name': st.column_config.Column(label="Участник", width='150px'), 
#         'best_time': st.column_config.Column(label="Лучшее время", width='100px'),
#         'finishes': st.column_config.Column(label="# финишей", width='100px'),
#         'peterhof_finishes_count': st.column_config.Column(label="# финишей в Петергофе", width='150px'),
#         'volunteers': st.column_config.Column(label="# волонтерств", width='120px'),
#         'peterhof_volunteers_count': st.column_config.Column(label="# волонтерств в Петергофе", width='150px'),
#         'clubs_titles': st.column_config.Column(label="Клубы", width='210px'),
#     },
#     hide_index=True
# )

# CSS для изменения ширины таблицы
table_css = """
    <style>
    .data-editor-container {
        width: 800px;  /* Ширина всей таблицы */
        margin: 0 auto;  /* Центрирование */
    }
    </style>
"""

# Отображаем CSS через markdown
st.markdown(table_css, unsafe_allow_html=True)

# Контейнер для таблицы с классом для применения стилей
with st.container():
    st.data_editor(
        df,
        column_config={
            'profile_link': st.column_config.LinkColumn(label="id 5Вёрст", display_text=r"([0-9]*)$", width='100px'),
            'name': st.column_config.Column(label="Участник", width='large'), 
            'best_time': st.column_config.Column(label="Лучшее время", width='100px'),
            'finishes': st.column_config.Column(label="# финишей", width='100px'),
            'peterhof_finishes_count': st.column_config.Column(label="# финишей в Петергофе", width='150px'),
            'volunteers': st.column_config.Column(label="# волонтерств", width='120px'),
            'peterhof_volunteers_count': st.column_config.Column(label="# волонтерств в Петергофе", width='150px'),
            'clubs_titles': st.column_config.Column(label="Клубы", width='large'),
        },
        hide_index=True,
        key="custom_table"
    )