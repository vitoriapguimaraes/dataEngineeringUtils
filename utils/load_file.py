import pandas as pd
import streamlit as st
import os


@st.cache_data(show_spinner=False)
def load_data(file_or_buffer):
    """
    Carrega dados de um arquivo CSV ou Excel.
    Aceita um caminho de arquivo (str) ou um buffer (UploadedFile).
    Retorna uma tupla: (DataFrame, Mensagem de Erro/Sucesso)
    """
    try:
        if isinstance(file_or_buffer, str):
            return _load_from_path(file_or_buffer)
        return _load_from_buffer(file_or_buffer)
    except Exception as e:
        return None, str(e)


def _load_from_path(file_path):
    if not os.path.exists(file_path):
        return None, f"Arquivo não encontrado: {file_path}"

    if _is_excel(file_path):
        return pd.read_excel(file_path), "Sucesso"

    return _read_csv_with_fallback(file_path)


def _load_from_buffer(buffer):
    filename = getattr(buffer, "name", "").lower()

    if _is_excel(filename):
        return pd.read_excel(buffer), "Sucesso"

    return _read_csv_with_fallback(buffer, is_buffer=True)


def _is_excel(filename):
    return filename.endswith(".xlsx") or filename.endswith(".xls")


def _read_csv_with_fallback(source, is_buffer=False):
    try:
        if is_buffer and hasattr(source, "seek"):
            source.seek(0)
        return pd.read_csv(source, encoding="utf-8"), "Sucesso"
    except UnicodeDecodeError:
        if is_buffer and hasattr(source, "seek"):
            source.seek(0)
        return pd.read_csv(source, encoding="latin-1"), "Sucesso"
