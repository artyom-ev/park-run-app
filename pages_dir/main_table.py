import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st

st.set_page_config(layout='wide', initial_sidebar_state='collapsed')

st.header('База участников 5Верст в Петергофе')

engine = create_engine('sqlite:///mydatabase.db')
    
querie = '''
SELECT profile_link, name, best_time, finishes, 
        peterhof_finishes_count, volunteers, peterhof_volunteers_count, clubs
FROM users
'''
df = pd.read_sql(querie, con=engine)

st.markdown(f'''
            Уникальных участников в таблице {len(df)}  
            ''')

# Отображаем таблицу 
st.data_editor(
    df,
    column_config={
        'profile_link': st.column_config.LinkColumn(label="id 5Вёрст", display_text=r"([0-9]*)$", width='small'),
        'name': st.column_config.Column(label="Участник", width='medium'), 
        # 'last_name': st.column_config.Column(label="Фамилия", width='medium'),     
        # 'first_name': st.column_config.Column(label="Имя", width='medium'),
        'best_time': st.column_config.Column(label="Лучшее время", width='small'),
        'finishes': st.column_config.Column(label="# финишей", width='medium'),
        'peterhof_finishes_count': st.column_config.Column(label="# финишей в Петергофе", width='medium'),
        'volunteers': st.column_config.Column(label="# волонтерств", width='medium'),
        'peterhof_volunteers_count': st.column_config.Column(label="# волонтерств в Петергофе", width='medium'),
        'clubs': st.column_config.Column(label="Клубы", width='medium'),
    },
    hide_index=True
)