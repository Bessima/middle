import sqlite3
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def extract():
    """
    extract data from sql-db
    :return:
    """
    connection = sqlite3.connect("db.sqlite")
    cursor = connection.cursor()

    """ 
    Лень порой является двигателем прогресса, но не в этом случае) 
    Тебе действительно тут нужно сделать один запрос - 
    посмотри в сторону использования оператора JOIN. 
    Вот тут есть сведения об данном операторе и его реализации в sqlite.
    Посмотри их, выбери подходящий для нашего случая вариант реализации JOIN 
    и перестрой запрос данных:
    https://www.sqlitetutorial.net/sqlite-inner-join/
    """
    # Наверняка это пилится в один sql - запрос, но мне как-то лениво)

    """
    Не забывай про стандарт оформления кода PEP8. 
    Если хочешь оставлять комментарии в коде, которые не будут дублировать твой код, 
    а будут помогать более точно разобраться с ним - то не забывай, 
    что длина строки комментария не должна превышать 74 символа 
    https://www.python.org/dev/peps/pep-0008/#maximum-line-length
    """
    # Получаем все поля для индекса, кроме списка актеров и сценаристов, для них только id
    cursor.execute("""
        select id, imdb_rating, genre, title, plot, director,
        -- comma-separated actor_id's
        (
            select GROUP_CONCAT(actor_id) from
            (
                select actor_id
                from movie_actors
                where movie_id = movies.id
            )
        ),

        max(writer, writers)
        from movies
    """)

    raw_data = cursor.fetchall()
    """
    Не стоит оставлять части закомментированного кода в своем проекте, 
    они ухудшают его чтение. Давай их уберем.
    """
    # cursor.execute('pragma table_info(movies)')
    # pprint(cursor.fetchall())

    """
    Давай добавим ещё один JOIN в твой основной SQL-запрос с таблицей actors. 
    Когда сделаешь JOIN, то не забудь указать условие, 
    которое у тебя уже есть на выборку данных из таблицы actors  
    """
    # Нужны для соответсвия идентификатора и человекочитаемого названия
    actors = {row[0]: row[1] for row in
              cursor.execute('select * from actors where name != "N/A"')}

    """
    Стоит учесть, что сценаристы могут повторяться, поэтому нужно осуществить выборку уникальных данных(без поторений). 
    Советую посмотреть в сторону использования дополнительных ключевых слов при операторе select. 
    https://www.sqlitetutorial.net/sqlite-select/
    https://www.sqlitetutorial.net/sqlite-select-distinct/
    """
    writers = {row[0]: row[1] for row in
               cursor.execute('select * from writers where name != "N/A"')}

    return actors, writers, raw_data


def transform(__actors, __writers, __raw_data):
    """

    :param __actors:
    :param __writers:
    :param __raw_data:
    :return:
    """
    documents_list = []
    for movie_info in __raw_data:
        """
        Давай уберем лишние комментарии, которые дублируют твой код.
        """
        # Разыменование списка
        """
        Хорошо, что ты тут используешь распаковку, а не обращаешься по индексам, 
        однако ты подзабыл про форматирование кода согласно стандартам:
        Согласно PEP8 длина строки кода не должна предышать 79 символов:
        https://www.python.org/dev/peps/pep-0008/#maximum-line-length
        """
        movie_id, imdb_rating, genre, title, description, director, raw_actors, raw_writers = movie_info

        if raw_writers[0] == '[':
            parsed = json.loads(raw_writers)
            new_writers = ','.join([writer_row['id'] for writer_row in parsed])
        else:
            new_writers = raw_writers

        writers_list = [(writer_id, __writers.get(writer_id)) for writer_id in
                        new_writers.split(',')]
        actors_list = [(actor_id, __actors.get(int(actor_id))) for actor_id in
                       raw_actors.split(',')]

        document = {
            "_index": "movies",
            "_id": movie_id,
            "id": movie_id,
            "imdb_rating": imdb_rating,
            "genre": genre.split(', '),
            "title": title,
            "description": description,
            "director": director,
            "actors": [
                {
                    "id": actor[0],
                    "name": actor[1]
                }
                for actor in set(actors_list) if actor[1]
            ],
            "writers": [
                {
                    "id": writer[0],
                    "name": writer[1]
                }
                for writer in set(writers_list) if writer[1]
            ]
        }

        for key in document.keys():
            if document[key] == 'N/A':
                """
                Нужно избавиться от закомментированного кода.
                """
                # print('hehe')
                document[key] = None

        document['actors_names'] = ", ".join(
            [actor["name"] for actor in document['actors'] if actor]) or None
        document['writers_names'] = ", ".join(
            [writer["name"] for writer in document['writers'] if
             writer]) or None

        """
        Тут у тебя несколько ошибок:
        1. Согласно PEP8 импорт всех нужных библиотек должен осуществляться в начале файла:
        https://www.python.org/dev/peps/pep-0008/#imports
        Учти это замечание на будущее
        2. В нашем задании необходимо организовать перезагрузку данных из sqlite в ES - 
        никаких дополнительных действий с кодом производить не нужно - 
        в частности вывод с помощью pprint нам тоже не нужен.
        Давай его уберем. 
        """
        import pprint
        pprint.pprint(document)

        documents_list.append(document)

    return documents_list


def load(acts):
    """
    У тебя получился какой-то не информативный docstrings.
    Кстати, проблемы оформления docstrings у тебя во всем файлике.
    Если решил его оформлять, то добавляй описание к функции, а также
    можно указать типизацию параметров ввода и вывод.
    Посмотри примеры правильного написания docstrings:
    https://www.python.org/dev/peps/pep-0257/#what-is-a-docstring
    Постарайся оформить по правилам - тебе это очень пригодится в твоей работе.
    Все публичные библиотеки оформляют docstrings, а современные IDE распознают
    и показывают информацию из docstrings во время использования некоторой функции.
    Для примера посмотьри исходный код библиотеки json, обрати внимание на оформление docstrings.
    """
    """

    :param acts:
    :return:
    """
    """
    Плохой практикой является указание напрямую различных системных 
    или секретных параметров (в нашем случае это параметры подключения).
    Давай лучше сделаем это через переменное окружение. 
    Почитай про использование библиотеки dotenv, 
    подключи её в своем проекте и исправь параметры подключения к ES
    https://preslav.me/2019/01/09/dotenv-files-python/
    """
    es = Elasticsearch([{'host': '192.168.1.252', 'port': 9200}])
    bulk(es, acts)

    return True


if __name__ == '__main__':
    load(transform(*extract()))