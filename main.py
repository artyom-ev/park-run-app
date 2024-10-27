import streamlit as st

st.switch_page('pages/about.py')  # for stramlit 1.32 - to work on iphone with old ios 
                 
# # --- PAGE SETUP --- not working in streamlit 1.32.0
# about_page = st.Page(
#     'pages_dir/about.py',
#     title='Домашняя страница',
#     icon='🏡',
#     default=True,
# )
# page_0 = st.Page(
#     'pages_dir/main_table.py',
#     title='Основная таблица',
#     icon='📝',
# )
# page_1 = st.Page(
#     'pages_dir/records_table.py',
#     title='Таблица рекордов',
#     icon='✨',
# )
# page_2 = st.Page(
#     'pages_dir/runs_table.py',
#     title='Таблица пробегов',
#     icon='🏃‍♀️',
# )
# page_3 = st.Page(
#     'pages_dir/orgs_table.py',
#     title='Таблица организаторов',
#     icon='💃',
# )

# # --- NAVIGATION SETUP [WITHOUT SECTIONS] ---
# pg = st.navigation(pages=[about_page, page_0, page_1, page_2, page_3])
 
# # --- RUN NAVIGATION ---
# pg.run()