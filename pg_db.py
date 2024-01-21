import json
import psycopg2
from loguru import logger
from config import hostname, username, password, database, table_name


class DatabaseManager:
    """
    Класс для управления базой данных PostgreSQL, включает в себя 
    функциональность подключения к базе данных, проверки существования таблицы и базы данных, 
    создания таблиц, поиска сотрудников и получения данных из таблицы.
    """

    def __init__(self):
        self.connection = self.connect_to_database()

    def connect_to_database(self):
        """
        Подключение к базе данных PostgreSQL.

        Возвращает:
            connection: объект соединения с базой данных.
        """
        return psycopg2.connect(
            host=hostname,
            user=username,
            password=password,
            dbname=database
        )

    def check_table_exists(self):
        """
        Проверяет, существует ли таблица с заданным именем в базе данных PostgreSQL.

        Параметры:
        - table_name (str): Имя таблицы для проверки.
        - connection: Соединение с базой данных PostgreSQL.

        Возвращает:
        - bool: True, если таблица существует, False в противном случае.
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = %s
                    );
                """, (table_name,))
                exists = cursor.fetchone()[0]
            return exists
        except (Exception, psycopg2.Error) as error:
            logger.error("Ошибка при выполнении запроса:", error)
            return False

    def create_table(self, data):
        """
        Создает таблицу используя предоставленные данные.

        Параметры:
            data (list): Список словарей с данными.
            table_name (str): Имя таблицы.
        """
        try:
            with self.connection.cursor() as cursor:

                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY,
                        ParentId INTEGER,
                        Name TEXT,
                        Type INTEGER,
                        CityId INTEGER
                    );
                    """)

                cursor.executemany(f"""
                    INSERT INTO {table_name} (id, ParentId, Name, Type, CityId)
                    VALUES (%(id)s, %(ParentId)s, %(Name)s, %(Type)s, %(CityId)s);
                """, data)

                self.connection.commit()

        except Exception as err:
            logger.error(err)

    def roommate_search(self, parent_id):
        """
        Ищет соседей (сотрудников с одинаковым ParentId) в базе данных.

        Параметры:
            parent_id (int): Идентификатор родителя, для которого ищутся соседи.

        Возвращает:
            List[Dict]: Список словарей, каждый из которых содержит информацию о сотруднике.
        """
        try:
            with self.connect_to_database() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT id, ParentId, Name, Type, CityId
                        FROM {table_name}
                        WHERE ParentId = %s;
                        """,
                        (parent_id,)
                    )

                    results = cursor.fetchall()
                    roommates = [
                        {'id': row[0], 'ParentId': row[1],
                            'Name': row[2], 'Type': row[3]}
                        for row in results
                    ]

                    return roommates

        except Exception as err:
            logger.error(err)
            return []

    def find_employee_by_employee_id(self, employee_id):
        """
        Находит сотрудника по идентификатору.

        Параметры:
            employee_id (int): ID сотрудника.

        Возвращает:
            dict: Информация о сотруднике, или None если сотрудник не найден.
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT id, ParentId, Name, Type, CityId
                    FROM {table_name}
                    WHERE id = %s;
                """, (employee_id,))

                result = cursor.fetchone()
                if result:
                    return {
                        'id': result[0],
                        'ParentId': result[1],
                        'Name': result[2],
                        'Type': result[3],
                        'CityId': result[4]
                    }
                else:
                    logger.info(f"Сотрудник с ID {employee_id} не найден.")
                    return None

        except Exception as err:
            logger.error(err)

    def get_all_data(self):
        """
        Получает все данные из таблицы.

        Возвращает:
            List[Dict]: Список с информацией о сотрудниках.
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT id, ParentId, Name, Type, CityId  
                                                   FROM {table_name}
                """)
                result = cursor.fetchall()
                employee_data = [
                    {
                        'id': row[0],
                        'ParentId': row[1],
                        'Name': row[2],
                        'Type': row[3],
                        'CityId': row[4]
                    } for row in result
                ]
                return employee_data
        except Exception as err:
            logger.error(err)
            return []

    def city_info_search(self, parent_id):
        """
        Поиск информации о городе по идентификатору родителя.

        Параметры:
            parent_id (int): Идентификатор родителя, для которого выполняется поиск информации о городе.

        Возвращает:
            dict or None: Словарь, содержащий информацию о городе (id, ParentId, Name, Type, CityId),
            если город найден, или None, если информация не найдена.
        """
        try:
            with self.connect_to_database() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT id, ParentId, Name, Type, CityId
                        FROM {table_name}
                        WHERE id = %s;
                        """,
                        (parent_id,)
                    )

                    result = cursor.fetchone()

                    if result:
                        city_info = {
                            'id': result[0],
                            'ParentId': result[1],
                            'Name': result[2],
                            'Type': result[3],
                            'CityId': result[4]
                        }
                        return city_info
                    else:
                        logger.info(
                            f"Информация о городе с родителем ID {parent_id} не найдена.")
                        return None

        except Exception as err:
            logger.error(err)
            return None

    def employee_search(self, parent_id):
        """
        Поиск города по офису и получение списка сотрудников в этом городе.

        Аргументы:
        - parent_id (int): Идентификатор офиса, для которого выполняется поиск.

        Возвращает:
        - tuple: Кортеж с названием города и списком сотрудников.

        Пример использования:
        ```
        city, employees = employee_search(2)
        print("Город:", city)
        print("Сотрудники:", employees)
        ```
        """
        # Определим к какому городу относится офис
        city_info = self.city_info_search(parent_id)
        # Идем по родственникам, пока не доберемся до города
        while city_info["Type"] != 1:
            city_info = self.city_info_search(city_info["ParentId"])

        # Поиск всех сотрудников этом городе по CityId
        try:
            with self.connect_to_database() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT id, ParentId, Name, Type, CityId
                        FROM {table_name}
                        WHERE CityId = %s;
                        """,
                        (city_info["id"],)
                    )

                    results = cursor.fetchall()
                    employees_in_city = [
                        {'id': row[0], 'ParentId': row[1],
                            'Name': row[2], 'Type': row[3], 'CityId': row[4]}
                        for row in results
                    ]

                    return city_info["Name"], employees_in_city
        except Exception as err:
            logger.error(err)
            return None, []

    def __del__(self):
        """
        Закрывает соединение с базой данных при уничтожении объекта класса.
        """
        try:
            if self.connection:
                self.connection.close()
                logger.info("Соединение с базой данных закрыто.")
        except Exception as err:
            logger.error(f"Ошибка при закрытии соединения: {err}")


db = DatabaseManager()
