from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection


def create_table_logging(engine: Engine) -> None:
    """
    Создание таблиц логов.

    :param engine: SQLAlchemy Engine объект
    :return: None
    """
    with engine.connect() as connect:
        with connect.begin():
            connect.execute(text("""
                CREATE TABLE IF NOT EXISTS logs.extract_load_logs (
                    action_date TIMESTAMP NOT NULL DEFAULT NOW(),
                    status VARCHAR(40),
                    description TEXT,
                    error TEXT
                );
            """))


def logging(status: str, engine: Engine, description: str = '', error: str = '') -> None:
    """
    Загрузка данных в таблицу логов.

    :param status: Статус загрузки данных (строка).
    :param engine: Объект SQLAlchemy Engine.
    :param description: Описание (по умолчанию пустая строка).
    :param error: Ошибка (по умолчанию пустая строка).
    :return: None
    """
    with engine.connect() as connect:  # type: Connection
        query = text("""
            INSERT INTO logs.extract_load_logs (status, description, error)
            VALUES (:status, :description, :error)
        """)

        connect.execute(query, status=status, description=description, error=error)
