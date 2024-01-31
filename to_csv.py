from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from bd_logging import logging
from utils import messages


def exist_table(engine: Engine, schema: str, table_name: str) -> bool:
    """
    Проверка существования витрины в PostgreSQL и данных в ней.

    :param engine: Объект SQLAlchemy Engine.
    :param schema: Схема витрины.
    :param table_name: Имя витрины.
    :return: True, если витрина существует и содержит данные, иначе False.
    """
    # Проверка существования таблицы
    with engine.begin() as connect:  # type: Connection
        exist_table = connect.execute(text(f"""select exists(
                                        select * 
                                        from information_schema.tables
                                        where table_schema = '{schema}' 
                                        and table_name = '{table_name}')""")).first()[0]
        # Проверка существования данных в ней и логирование ошибок
        if exist_table:
            data_query = text(f"""SELECT * FROM {schema}.{table_name};""")
            data = connect.execute(data_query).scalar()
        # Логирование ошибок на случай если в таблице нет данных
            if data is not None:
                return True
            else:
                logging('error_exist_table_function', engine,
                        f'Ошибка, в витрине {schema}.{table_name} отсутствуют данные', '')
                print(f'Ошибка, в витрине {schema}.{table_name} отсутствуют данные.\n')
                messages.append(f'Ошибка, в витрине {schema}.{table_name} отсутствуют данные.\n')
        else:
            logging('error_exist_table_function', engine,
                    f'Ошибка, витрины {schema}.{table_name} не существует.')
            print(f'Ошибка, витрины {schema}.{table_name} не существует.')
            messages.append(f'Ошибка, витрины {schema}.{table_name} не существует.')

        return False


def extract_postgresql(engine: Engine, schema: str, table_name: str) -> Optional[pd.DataFrame]:
    """
    Извлечение данных из PostgreSQL.

    :param engine: Объект SQLAlchemy Engine.
    :param schema: Схема витрины.
    :param table_name: Имя витрины.
    :return: DataFrame с извлеченными данными или None, если произошла ошибка.
    """
    try:
        connection = engine.connect()  # Создание объекта Connection
        query = f'SELECT * FROM {schema}.{table_name}'
        result = connection.execute(text(query))  # Использование объекта Connection для выполнения запроса
        df = pd.DataFrame(result.fetchall(), columns=result.keys())  # Преобразование результата запроса в DataFrame
        connection.close()  # Закрытие соединения

        logging('extract_postgresql_function', engine,
                f'Извлечение данных из витрины {schema}.{table_name} PostgreSQL выполнено успешно.')
        print(f'Извлечение данных из витрины {schema}.{table_name} PostgreSQL выполнено успешно. \n')
        messages.append(f'Извлечение данных из витрины {schema}.{table_name} PostgreSQL выполнено успешно. \n')

        return df
    except Exception as e:
        logging('error_extract_postgresql_function', engine,
                f'Ошибка при извлечении данных из витрины {schema}.{table_name} PostgreSQL: {e}')
        print(f'Ошибка при извлечении данных из витрины {schema}.{table_name} PostgreSQL: {e} \n')
        messages.append(f'Ошибка при извлечении данных из витрины {schema}.{table_name} PostgreSQL: {e} \n')
        return None


def upload_to_csv(engine: Engine, df: pd.DataFrame, table_name: str, file_to_open: str) -> None:
    """
    Загрузка данных в CSV файл.

    :param engine: Объект SQLAlchemy Engine.
    :param df: DataFrame, содержащий данные для сохранения в CSV.
    :param table_name: Имя таблицы или витрины.
    :param file_to_open: Путь и имя файла CSV, в который будут сохранены данные.
    :return: None
    """
    try:
        df.to_csv(file_to_open, sep=';', encoding='utf-8', index=False)
        logging('upload_to_csv_function', engine, f'Данные загружены в файл {table_name}.csv.')
        print(f'Данные загружены в файл {table_name}.csv.\n')
        messages.append(f'Данные загружены в файл {table_name}.csv.\n')
    except Exception as e:
        logging('error_upload_to_csv_function', engine, f'Ошибка при загрузке данных в файл {table_name}.csv: {e}')
        print(f'Ошибка при загрузке данных в файл {table_name}.csv: {e}\n')
        messages.append(f'Ошибка при загрузке данных в файл {table_name}.csv: {e}\n')
