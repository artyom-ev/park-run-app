import streamlit as st

def menu():
    st.sidebar.page_link("pages/about.py", label="Домашняя")
    st.sidebar.page_link("pages/main_table.py", label="Таблица участников")
    st.sidebar.page_link("pages/records_table.py", label="Клубы и рекорды")