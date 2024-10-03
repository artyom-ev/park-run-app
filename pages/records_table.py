import pandas as pd
import streamlit as st

# Установка конфигурации страницы
st.set_page_config(layout='wide', initial_sidebar_state='collapsed')

# Заголовок
st.title('Таблицы по новичкам, рекордсменам и вступившим в клубы 10/20/50/100')

# Чтение данных из CSV файлов
df_runners = pd.read_csv('runners.csv')
df_orgs = pd.read_csv('organizers.csv')

# Преобразуем run_date в формат datetime для корректной фильтрации
df_runners['run_date'] = pd.to_datetime(df_runners['run_date'], dayfirst=True)

st.header('Бегуны')

# Фильтрация данных
latest_date = df_runners['run_date'].max()  # Получаем максимальную дату

# Проверяем, есть ли данные
if not df_runners.empty:
    st.write("Последняя дата:", latest_date)  # Вывод последней даты для проверки

    filtered_df = df_runners[
        (
            df_runners['finishes'].isin(['10 финишей', '20 финишей', '50 финишей', '100 финишей']) |
            df_runners['volunteers'].isin(['10 волонтёрств', '20 волонтёрств', '50 волонтёрств', '100 волонтёрств']) |
            (df_runners['achievements'].notnull() & df_runners['achievements'].str.strip().ne(''))
        ) & 
        (df_runners['run_date'] == latest_date)
    ].copy()  # Создаем копию

    # Извлекаем имя и фамилию
    if not filtered_df.empty:
        filtered_df['first_name'] = filtered_df['name'].str.split().str[0]
        filtered_df['last_name'] = filtered_df['name'].str.split().str[1]

        # Оставляем только нужные колонки
        final_df = filtered_df[['participant_id', 'first_name', 'last_name', 'finishes', 'volunteers', 'achievements', 'profile_link']]

        # Используем st.data_editor для отображения таблицы
        st.data_editor(
            final_df,
            column_config={
                'profile_link': st.column_config.LinkColumn(),
            },
            hide_index=True
        )

    else:
        st.write("Нет данных, соответствующих критериям фильтрации для бегунов.")
else:
    st.write("Нет данных для бегунов.")

st.header('Организаторы')

# Преобразуем run_date в формат datetime для корректной фильтрации
df_orgs['run_date'] = pd.to_datetime(df_orgs['run_date'], dayfirst=True)
df_orgs['participant_id'] = df_orgs['participant_id'].astype(str)

# Фильтрация данных
latest_date_orgs = df_orgs['run_date'].max()  # Получаем максимальную дату

# Проверяем, есть ли данные
if not df_orgs.empty:
    st.write("Последняя дата для организаторов:", latest_date_orgs)  # Вывод последней даты для проверки

    filtered_orgs_df = df_orgs[
        (
            (df_orgs['first_volunteer_info'].notnull() & 
            df_orgs['first_volunteer_info'].str.strip().ne('') & 
            df_orgs['first_volunteer_info'].ne('—')) & 
            (df_orgs['run_date'] == latest_date_orgs)
        )
    ].copy()  # Создаем копию

    # Извлекаем имя и фамилию
    if not filtered_orgs_df.empty:
        filtered_orgs_df['first_name'] = filtered_orgs_df['name'].str.split().str[0]
        filtered_orgs_df['last_name'] = filtered_orgs_df['name'].str.split().str[1]

        # Оставляем только нужные колонки
        final_orgs_df = filtered_orgs_df[['participant_id', 'first_name', 'last_name', 'finishes', 'volunteers', 'first_volunteer_info', 'profile_link']]

        # Используем st.data_editor для отображения таблицы
        st.data_editor(
            final_orgs_df,
            column_config={
                'profile_link': st.column_config.LinkColumn(),
            },
            hide_index=True
        )
    else:
        st.write("Нет данных, соответствующих критериям фильтрации для организаторов.")
else:
    st.write("Нет данных для организаторов.")

# Создание дополнительной таблицы в конце
if not df_runners.empty:
    # Создаем списки для каждой категории
    newcomers = filtered_df[filtered_df['achievements'].str.contains('Первый финиш', na=False)]['name'].tolist()
    record_holders = filtered_df[filtered_df['achievements'].str.contains('Личный рекорд', na=False)]['name'].tolist()
    run_clubs = filtered_df[filtered_df['finishes'].isin(['10 финишей', '20 финишей', '50 финишей', '100 финишей'])]['name'].tolist()
    volunteer_clubs = filtered_df[filtered_df['volunteers'].isin(['10 волонтёрств', '20 волонтёрств', '50 волонтёрств', '100 волонтёрств'])]['name'].tolist()

    # Найдем максимальную длину среди всех списков
    max_length = max(len(newcomers), len(record_holders), len(run_clubs), len(volunteer_clubs))

    # Создаем DataFrame с равной длиной
    new_table = pd.DataFrame({
        'Новички': pd.Series(newcomers + [''] * (max_length - len(newcomers))),
        'Рекордсмены': pd.Series(record_holders + [''] * (max_length - len(record_holders))),
        'Клубы забега': pd.Series(run_clubs + [''] * (max_length - len(run_clubs))),
        'Клубы волонтёрства': pd.Series(volunteer_clubs + [''] * (max_length - len(volunteer_clubs))),
    })

    # Отображаем новую таблицу
    st.header('Общая таблица')
    st.dataframe(new_table)