from sqlalchemy.engine import Engine
from os import getenv
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine

from utils import messages


def connect_to_postgresql() -> Engine:
    """
    Проверка подключения к PostgreSQL.

    :return: SQLAlchemy Engine объект для подключения к PostgreSQL.
    """
    try:
        # Для использования переменных окружения
        load_dotenv(find_dotenv())

        # Переменные окружения
        DB_NAME = getenv("DB_NAME")
        DB_HOST = getenv("DB_HOST")
        DB_PORT = getenv("DB_PORT")
        DB_USER = getenv("DB_USER")
        DB_PASSWORD = getenv("DB_PASSWORD")

        # Создание подключения к PostgreSQL
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

        # Подключение к PostgreSQL
        engine.connect()

        # Логирование
        print('\n Подключение к PostgreSQL успешно \n')
        messages.append('\n Подключение к PostgreSQL успешно \n')
        return engine

    except Exception as e:
        # Логирование и обработка ошибок
        print(f'\n Ошибка подключения к PostgreSQL: \n {e} \n')
        messages.append(f'\n Ошибка подключения к PostgreSQL: \n {e} \n')
