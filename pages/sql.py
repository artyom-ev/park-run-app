import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# Настройка подключения к базе данных
db_url = 'sqlite:///mydatabase.db'
engine = create_engine(db_url)

# Функция для выполнения кастомного SQL-запроса
def run_custom_query(query):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            # Если запрос возвращает данные (SELECT)
            if result.returns_rows:
                # Преобразуем результат в DataFrame
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
            else:
                return "Запрос выполнен успешно."
    except Exception as e:
        return f"Ошибка выполнения запроса: {e}"

# Интерфейс приложения Streamlit
st.title("Кастомные SQL-запросы к базе данных")

# Поле для ввода кастомного SQL-запроса
query = st.text_area("Введите ваш SQL-запрос:")

# Кнопка для выполнения запроса
if st.button("Выполнить запрос"):
    if query.strip():  # Проверяем, что запрос не пустой
        # Выполняем запрос и выводим результат
        result = run_custom_query(query)
        
        # Если результат — DataFrame, отображаем его
        if isinstance(result, pd.DataFrame):
            st.write("Результаты запроса:")
            st.dataframe(result)
        else:
            st.write(result)
    else:
        st.warning("Введите SQL-запрос.")