from flask import Flask, abort, request, jsonify
import elasticsearch as ES

from validate import validate_args

app = Flask(__name__)

"""
Хорошо было бы добавить логирование. Прочитай и подключи тут модуль logging
Вот хорошая статья на русском по этому модулю:
https://medium.com/nuances-of-programming/%D0%BB%D0%BE%D0%B3%D0%B8-%D0%B2-python-%D0%BD%D0%B0%D1%81%D1%82%D1%80%D0%BE%D0%B9%D0%BA%D0%B0-%D0%B8-%D1%86%D0%B5%D0%BD%D1%82%D1%80%D0%B0%D0%BB%D0%B8%D0%B7%D0%B0%D1%86%D0%B8%D1%8F-a3cf257da1b8

"""


"""
Согласно условию задачи нам нужно реализовать поиск фильмов и получение делатьной фильма, 
поэтому запрос главной страницы можно было не делать. Но то, что его добавил - ошибкой не является) 
"""
@app.route('/')
def index():
    return 'worked'

@app.route('/api/movies/')
def movie_list():
    validate = validate_args(request.args)

    if not validate['success']:
        return abort(422)

    defaults = {
        """
        Применение магических чисел в коде является плохой практикой. 
        Давай лучше вынесем на глобальный уровень в константы дефолтные параметры - limit и page.
        """
        'limit': 50,
        'page': 1,
        'sort': 'id',
        'sort_order': 'asc'
    }

    """
    Данный комментарий лучше убрать - он не дает полезную информацию, которая помогла бы понять твой код.
    """
    # Тут уже валидно все
    for param in request.args.keys():
        defaults[param] = request.args.get(param)
    """
    Если хочешь оставлять комментарии в коде, которые не будут дублировать твой код, 
    а будут помогать более точно разобраться с ним - то не забывай, 
    что длина строки комментария не должна превышать 74 символа 
    https://www.python.org/dev/peps/pep-0008/#maximum-line-length
    """
    # Уходит в тело запроса. Если запрос не пустой - мультисерч, если пустой - выдает все фильмы
    body = {
        "query": {
            "multi_match": {
                "query": defaults['search'],
                "fields": ["title"]
            }
        }
    } if defaults.get('search', False) else {}

    body['_source'] = dict()
    body['_source']['include'] = ['id', 'title', 'imdb_rating']

    params = {
        """
        Закомментированный код лучше убрать, чтобы он не мешал чтению твоего кода.
        """
        # '_source': ['id', 'title', 'imdb_rating'],
        'from': int(defaults['limit']) * (int(defaults['page']) - 1),
        'size': defaults['limit'],
        'sort': [
            {
                defaults["sort"]: defaults["sort_order"]
            }
        ]
    }
    """
        Плохой практикой является указание напрямую различных системных
        или секретных параметров (в нашем случае это параметры подключения).
        Давай лучше сделаем это через переменное окружение.
        Почитай про использование библиотеки dotenv,
        подключи её в своем проекте и исправь параметры подключения к ES
        https://preslav.me/2019/01/09/dotenv-files-python/
    """
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )
    search_res = es_client.search(
        body=body,
        index='movies',
        params=params,
        filter_path=['hits.hits._source']
    )
    es_client.close()

    return jsonify([doc['_source'] for doc in search_res['hits']['hits']])


@app.route('/api/movies/<string:movie_id>')
def get_movie(movie_id):
    """
    Код с подключением к ES повторяется.
    Давай создадим отдельную функцию, в которую поместим подключение к ES,
    и будем вызывать эту функцию там, где нужно.
    """
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )

    if not es_client.ping():
        print('oh(')

    search_result = es_client.get(index='movies', id=movie_id, ignore=404)

    es_client.close()

    if search_result['found']:
        return jsonify(search_result['_source'])

    return abort(404)

if __name__ == "__main__":
    """
    Лучше выносить системные параметры в файл, который потом обрабатывается dotenv. 
    Если некоторые параметры не являются секретными или системными, 
    и скорее всего будут идентичными для запуска на любой машине, 
    то лучше их оформлять в виде констант с соответствующими именами. 
    Такая формализация поможет избежать использования магических чисел и магических строк, 
    которые являются плохой практикой.    
    """
    app.run(host='0.0.0.0', port=80)