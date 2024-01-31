import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from bd_logging import logging
from utils import messages


def upload_to_postgresql(engine: Engine, schema: str, table_name: str, copy_table_name: str, file_to_open: str) -> None:
    """
    Выгрузка данных из CSV в PostgreSQL.

    :param engine: Объект SQLAlchemy Engine.
    :param schema: Схема PostgreSQL.
    :param table_name: Имя таблицы PostgreSQL, из которой происходит выгрузка.
    :param copy_table_name: Имя таблицы PostgreSQL, в которую происходит выгрузка.
    :param file_to_open: Путь к файлу CSV для загрузки данных.
    :return: None
    """
    try:
        df = pd.read_csv(file_to_open, sep=';', encoding='utf-8')
        with engine.connect() as connect:  # type: Connection
            connect.execute(text(f"""DROP TABLE IF EXISTS {schema}.{copy_table_name};"""))
            connect.execute(text(f"""CREATE TABLE IF NOT EXISTS {schema}.{copy_table_name} AS TABLE {schema}.{table_name} WITH NO DATA;"""))

        df.to_sql(copy_table_name, engine, if_exists='append', schema=schema, index=False)

        logging('upload_postgresql_function', engine, f'Данные в таблицу {schema}{copy_table_name} успешно записаны.')
        print(f'Данные в таблицу {schema}{copy_table_name} успешно записаны.\n')
        messages.append(f'Данные в таблицу {schema}{copy_table_name} успешно записаны.\n')
    except Exception as e:
        logging('error_upload_postgresql_function', engine, f'Ошибка выгрузки данных из файла .csv: {e}')
        print(f"Ошибка выгрузки данных из файла .csv: {e}\n")
        messages.append(f"Ошибка выгрузки данных из файла .csv: {e}\n")
