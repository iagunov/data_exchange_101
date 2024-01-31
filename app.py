import sys
import time
from pathlib import Path
from typing import Optional
from flask import Flask, render_template, request
import pandas as pd
from sqlalchemy.engine import Engine

from bd_logging import create_table_logging
from postgresql_connection import connect_to_postgresql
from to_csv import exist_table, extract_postgresql, upload_to_csv
from to_postgresql import upload_to_postgresql
from utils import messages

app = Flask(__name__, template_folder='templates')


# Создание маршрута веб интерфейса
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/run_script', methods=['POST'])
def run_script() -> str:
    """
    Запуск скрипта.
    :return: None
    """
    start_time = time.time()
    # Название таблицы и схем в БД
    schema = 'dm'
    table_name = 'dm_f101_round_f'
    copy_table_name = 'dm_f101_round_f_v2'
    # Путь к файлу
    file_to_open = f"{Path(sys.path[0], 'data', table_name)}.csv"
    keyboard_input = int(request.form['mode'])

    engine: Optional[Engine] = connect_to_postgresql()

    # Если удалось подключится к БД создаем таблицу логов если ее нет
    if engine:
        create_table_logging(engine)
        # Проверяем таблицу в БД на существование
        if exist_table(engine, schema, table_name):
            # При выборе опции 1 читаем данные из БД и пишем в файл
            if keyboard_input == 1:
                df: Optional[pd.DataFrame] = extract_postgresql(engine, schema, table_name)
                upload_to_csv(engine, df, table_name, file_to_open)
                print("--- %s seconds ---" % (time.time() - start_time))
                messages.append("--- %s seconds ---" % (time.time() - start_time))
            # При выборе опции 2 читаем данные из файла и пишем в БД
            elif keyboard_input == 2:
                upload_to_postgresql(engine, schema, table_name, copy_table_name, file_to_open)
                print("--- %s seconds ---" % (time.time() - start_time))
                messages.append("--- %s seconds ---" % (time.time() - start_time))
    # Отправляем сообщения в интерфейс
    return render_template('index.html', messages=messages)


if __name__ == '__main__':
    app.run(debug=True)
