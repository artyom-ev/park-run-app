import pandas as pd
import streamlit as st

st.set_page_config(layout='wide', initial_sidebar_state='collapsed')

st.title('Основная таблица по результатам и волонтёрствам участников')

# Чтение данных из CSV файлов
df_runners = pd.read_csv('runners.csv')
df_orgs = pd.read_csv('organizers.csv')

# Преобразуем participant_id в строку
df_runners['participant_id'] = df_runners['participant_id'].astype(str)
df_orgs['participant_id'] = df_orgs['participant_id'].astype(str)

# Преобразуем строковый формат времени в datetime
df_runners['time'] = pd.to_datetime(df_runners['time'], format='%H:%M:%S', errors='coerce')

# Группировка данных по бегунам
runners_grouped = df_runners.groupby(['participant_id', 'name', 'profile_link']).agg(
    min_time=('time', 'min'),
    number_of_runs=('run_date', 'nunique'),
    number_of_runs_in_peterhof=('run', lambda x: (x == 'Петергоф Александрийский').sum())
).reset_index()

# Группировка данных по организаторам
organizers_grouped = df_orgs.groupby(['participant_id', 'name', 'profile_link']).agg(
    number_of_helps=('run_date', 'nunique'),
    number_of_helps_in_peterhof=('run', lambda x: (x == 'Петергоф Александрийский').sum())
).reset_index()

# Разбиваем имя на first_name и last_name
runners_grouped['first_name'] = runners_grouped['name'].str.split().str[0]
runners_grouped['last_name'] = runners_grouped['name'].str.split().str[1]
organizers_grouped['first_name'] = organizers_grouped['name'].str.split().str[0]
organizers_grouped['last_name'] = organizers_grouped['name'].str.split().str[1]

# Объединяем данные
merged_df = pd.merge(runners_grouped, organizers_grouped, 
                     on=['participant_id', 'profile_link', 'first_name', 'last_name'], 
                     how='outer', suffixes=('_runner', '_organizer'))

# Заполняем пропуски без inplace=True
merged_df['min_time'] = merged_df['min_time'].fillna(pd.Timestamp('00:00:00'))
merged_df['number_of_runs'] = merged_df['number_of_runs'].fillna(0)
merged_df['number_of_runs_in_peterhof'] = merged_df['number_of_runs_in_peterhof'].fillna(0)
merged_df['number_of_helps'] = merged_df['number_of_helps'].fillna(0)
merged_df['number_of_helps_in_peterhof'] = merged_df['number_of_helps_in_peterhof'].fillna(0)

# Преобразуем min_time
merged_df['min_time'] = merged_df['min_time'].dt.strftime('%H:%M:%S')

# Оставляем нужные колонки
final_df = merged_df[['participant_id', 'first_name', 'last_name', 'min_time', 
                      'number_of_runs', 'number_of_runs_in_peterhof', 
                      'number_of_helps', 'number_of_helps_in_peterhof', 'profile_link']]

# Используем st.data_editor для отображения таблицы
st.data_editor(
    final_df,
    column_config={
        'profile_link': st.column_config.LinkColumn(),
    },
    hide_index=True
)