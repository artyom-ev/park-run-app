import streamlit as st

def menu():
    st.sidebar.page_link('pages/home.py', label='Домашняя')
    st.sidebar.page_link('pages/main_table.py', label='Таблица участников')
    st.sidebar.page_link('pages/records.py', label='Клубы и рекорды')