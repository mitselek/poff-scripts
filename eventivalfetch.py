import os, sys
import urllib.request, json, csv

import time
from functools import wraps

import xmltodict

from bs4 import BeautifulSoup

def retry(exceptions, tries=4, delay=3, backoff=2, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay each retry).
        logger: Logger to use. If None, print.
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = '{}, Retrying in {} seconds...'.format(e, mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry

@retry(urllib.error.HTTPError, tries=5, delay=1, backoff=1.2)
def urlopen_with_retry(userUrl):
    return urllib.request.urlopen(userUrl)

datadir = 'data'
db = {
    'host': os.getenv('FILMS_DB_HOST'),
    'user': os.getenv('FILMS_DB_USER'),
    'passwd': os.getenv('FILMS_DB_PASSWORD'),
    'database': os.getenv('FILMS_DB_NAME')
}
import mysql.connector

mydb = mysql.connector.connect(
  host = db['host'],
  user = db['user'],
  passwd = db['passwd'],
  database = db['database']
)
# print(mydb)
mycursor = mydb.cursor()

# Eventival subfestival codes
subfests = {
    1839: 'Shorts',
    10: 'PÖFF',
    9: 'Just Film',
}

tasks = {
    'venues' : {
        'url': 'https://eventival.eu/poff/23/en/ws/VYyOdFh8AFs6XBr7Ch30tu12FljKqS/venues.xml',
        'json': 'venues.json',
        'root_path': 'venues.venue'
    },
    'publications' : {
        'url': 'https://eventival.eu/poff/23/en/ws/VYyOdFh8AFs6XBr7Ch30tu12FljKqS/films/categories/{subfest}/publications-locked.xml',
        'json': 'publications.json',
        'root_path': 'films.item'
    },
    'screenings' : {
        'url': 'https://eventival.eu/poff/23/en/ws/VYyOdFh8AFs6XBr7Ch30tu12FljKqS/films/categories/{subfest}/screenings.xml',
        'json': 'screenings.json',
        'root_path': 'screenings.screening'
    }
}

def fetch_base(subfest):
    for task in tasks:
        root_path = tasks[task]['root_path'].split('.')
        userUrl = tasks[task]['url'].format(subfest=subfest)
        json_fn = os.path.join(datadir, tasks[task]['json'])
        print ('Fetch ' + userUrl + ' to ' + json_fn)

        with urlopen_with_retry(userUrl) as url:
            data = url.read()
            # print('Got {len} bytes worth of HTTP data'.format(len=len(data)))
        XML_data = data.decode()
        # print('Got {len} bytes worth of XML_data'.format(len=len(XML_data)))

        dict_data = xmltodict.parse(XML_data)
        for elem in root_path:
            dict_data = dict_data[elem]
        # print('Got {len} bytes worth of JSON'.format(len=len(json.dumps(dict_data))))

        with open(json_fn, 'w') as json_file:
            json.dump(dict_data, json_file, indent=4)
            print ('Done with ' + json_fn)

        globals()['parse_' + task](dict_data, task)


def parse_venues(dict_data, task):
    print('Parse ' + task)
    if not isinstance(dict_data, list):
        dict_data = [dict_data]

    # return
    queries = [
        {
            'mappings': {
                'id': 'id',
                'name': 'name',
                'company': 'company',
                'company_id': 'company_id',
                'city': 'company_contact.address.city'
            },
            'SQL': """INSERT IGNORE INTO venues (id, name, company, company_id, city)
                VALUES (%(id)s, %(name)s, %(company)s, %(company_id)s, %(city)s)
                ON DUPLICATE KEY UPDATE
                name=%(name)s, company=%(company)s, company_id=%(company_id)s, city=%(city)s
            ;"""
        }
    ]

    for item in dict_data:
        for query in queries:
            SQL = query['SQL']
            mappings = query['mappings']
            # print('item:', item)
            map = {}
            for mapping in mappings:
                path = mappings[mapping].split('.')
                elem = path.pop(0)
                # print('elem:', elem)
                if elem in item:
                    value = item[elem]
                # print('value:', value)
                for elem in path:
                    # print('2elem:', elem)
                    if value and elem in value:
                        value = value[elem]
                # print('map:', mapping, '<-', mappings[mapping], ' = ', value)
                map[mapping] = value
            if SQL:
                mycursor.execute(SQL, map)
                # print(mycursor.statement)
        # print('commit')
        mydb.commit()


def parse_publications(dict_data, task):
    print('Parse ' + task)
    # print('dd', dict_data)
    if not isinstance(dict_data, list):
        dict_data = [dict_data]
        # print('is list?', isinstance(dict_data, list))
    # return

    SQL = """INSERT IGNORE INTO films (id, title_eng, title_original)
        VALUES (%(id)s, %(title_eng)s, %(title_original)s)
        ON DUPLICATE KEY UPDATE
        title_eng=%(title_eng)s, title_original=%(title_original)s
    ;"""

    for item in dict_data:
        # print('item', item)
        map = { 'id': item['id'],
                'title_eng': item['title_english'],
                'title_original': item['title_original'] }
        mycursor.execute(SQL, map)
        # print(mycursor.statement)
        mydb.commit()

        fetch_film(item['id'])

    print('- Films committed')

    # Categories
    SQLs = [
        """INSERT IGNORE INTO categories (id, Category_est)
        VALUES (%(id)s, %(Category_est)s)
        ON DUPLICATE KEY UPDATE
        Category_est=%(Category_est)s
        ;""",
        """INSERT IGNORE INTO film_categories (film_id, category_id)
        VALUES (%(film_id)s, %(id)s)
        ;"""
    ]
    for item in dict_data:
        try:
            categories = item['eventival_categorization']['categories']['category']
        except Exception as e:
            print('No categories, skipping ', item)
            continue
        if not isinstance(categories, list):
            categories = [categories]
        for category in categories:
            map = { 'id': category['@id'], 'Category_est': category['#text'], 'film_id': item['id'] }
            for SQL in SQLs:
                mycursor.execute(SQL, map)
                # print(mycursor.statement)
        mydb.commit()
    print('- Categories committed')

    # Programs
    SQLs = [
        """INSERT IGNORE INTO programs (id, Program_est)
        VALUES (%(id)s, %(Program_est)s)
        ON DUPLICATE KEY UPDATE
        Program_est=%(Program_est)s
        ;""",
        """INSERT IGNORE INTO film_programs (film_id, program_id)
        VALUES (%(film_id)s, %(id)s)
        ;"""
        ]
    for item in dict_data:
        try:
            programs = item['eventival_categorization']['sections']['section']
        except Exception as e:
            print('No sections, skipping ', item)
            continue

        if not isinstance(programs, list):
            programs = [programs]
        for program in programs:
            map = { 'id': program['id'], 'Program_est': program['name'], 'film_id': item['id'] }
            for SQL in SQLs:
                mycursor.execute(SQL, map)
                # print(mycursor.statement)
        mydb.commit()
    print('- Programs committed')


def parse_screenings(dict_data, task):
    print('Parse ' + task)
    if not isinstance(dict_data, list):
        dict_data = [dict_data]
    # return

    SQL = """INSERT IGNORE INTO screenings (
            id, code, film_id, cinema_hall_id, venue_id,
            start_date, start_time,
            screening_duration_minutes, presentation_duration_minutes, qa_duration_minutes,
            type_of_screening, ticketing_url)
        VALUES (
            %(id)s, %(code)s, %(film_id)s, %(cinema_hall_id)s, %(venue_id)s,
            %(start_date)s, %(start_time)s,
            %(screening_duration_minutes)s, %(presentation_duration_minutes)s, %(qa_duration_minutes)s,
            %(type_of_screening)s, %(ticketing_url)s)
        ON DUPLICATE KEY UPDATE
            code=%(code)s, film_id=%(film_id)s, cinema_hall_id=%(cinema_hall_id)s, venue_id=%(venue_id)s,
            start_date=%(start_date)s, start_time=%(start_time)s,
            screening_duration_minutes=%(screening_duration_minutes)s,
            presentation_duration_minutes=%(presentation_duration_minutes)s,
            qa_duration_minutes=%(qa_duration_minutes)s,
            type_of_screening=%(type_of_screening)s, ticketing_url=%(ticketing_url)s
    ;"""
    i = 0
    for item in dict_data:
        i+=1
        # continue
        map = { 'id': item['id'], 'code': item['code'], 'film_id': item['film']['id'],
                'cinema_hall_id': item['cinema_hall_id'], 'venue_id': item['venue_id'],
                'start_date': item['start'][:10], 'start_time': item['start'][11:],
                'screening_duration_minutes': item['duration_screening_only_minutes'],
                'presentation_duration_minutes': item['presentation']['duration'],
                'qa_duration_minutes': item['qa']['duration'],
                'type_of_screening': item['type_of_screening'], 'ticketing_url': item['ticketing_url'] }
        mycursor.execute(SQL, map)
        # print(i, mycursor.statement)
        mydb.commit()
    print('- Screenings committed')

    # Persons
    SQLs = [
        """INSERT IGNORE INTO persons (id, name)
        VALUES (%(person_id)s, %(person_name)s)
        ;""",
        """INSERT IGNORE INTO screening_persons (screening_id, person_id, relation_id, part, role)
        SELECT %(screening_id)s, %(person_id)s, id, %(part)s, %(role)s
        FROM relations
        WHERE name=%(relation_name)s
        ;"""
        ]
    for item in dict_data:
        if item['presentation']['presenters']:
            (part, role) = ('presentation', 'presenter')
            presenters = item['presentation']['presenters']['person']
            if not isinstance(presenters, list):
                presenters = [presenters]
            for presenter in presenters:
                if presenter['relations'] is None:
                    presenter['relations'] = {'relation':['']}
                relations = presenter['relations']['relation']
                if not isinstance(relations, list):
                    relations = [relations]
                for relation in relations:
                    map = { 'person_id': presenter['@id'], 'person_name': presenter['name'],
                        'screening_id': item['id'], 'part': part, 'role': role, 'relation_name': relation }
                    for SQL in SQLs:
                        # print('got presenter for presentation for screening', SQL, map)
                        mycursor.execute(SQL, map)
                        # print(mycursor.statement)
        if item['presentation']['guests']:
            (part, role) = ('presentation', 'guest')
            guests = item['presentation']['guests']['person']
            if not isinstance(guests, list):
                guests = [guests]
            for guest in guests:
                if guest['relations'] is None:
                    guest['relations'] = {'relation':['']}
                relations = guest['relations']['relation']
                if not isinstance(relations, list):
                    relations = [relations]
                for relation in relations:
                    map = { 'person_id': guest['@id'], 'person_name': guest['name'],
                        'screening_id': item['id'], 'part': part, 'role': role, 'relation_name': relation }
                    for SQL in SQLs:
                        # print('got guest for presentation for screening', SQL, map)
                        mycursor.execute(SQL, map)
                        # print(mycursor.statement)
        if item['qa']['presenters']:
            (part, role) = ('qa', 'presenter')
            presenters = item['qa']['presenters']['person']
            if not isinstance(presenters, list):
                presenters = [presenters]
            for presenter in presenters:
                if presenter['relations'] is None:
                    presenter['relations'] = {'relation':['']}
                relations = presenter['relations']['relation']
                if not isinstance(relations, list):
                    relations = [relations]
                for relation in relations:
                    map = { 'person_id': presenter['@id'], 'person_name': presenter['name'],
                        'screening_id': item['id'], 'part': part, 'role': role, 'relation_name': relation }
                    for SQL in SQLs:
                        # print('got presenter for qa for screening', SQL, map)
                        mycursor.execute(SQL, map)
                        # print(mycursor.statement)
        if item['qa']['guests']:
            (part, role) = ('qa', 'guest')
            guests = item['qa']['guests']['person']
            if not isinstance(guests, list):
                guests = [guests]
            for guest in guests:
                if guest['relations'] is None:
                    guest['relations'] = {'relation':['']}
                relations = guest['relations']['relation']
                if not isinstance(relations, list):
                    relations = [relations]
                for relation in relations:
                    map = { 'person_id': guest['@id'], 'person_name': guest['name'],
                        'screening_id': item['id'], 'part': part, 'role': role, 'relation_name': relation }
                    for SQL in SQLs:
                        # print('got guest for qa for screening', SQL, map)
                        mycursor.execute(SQL, map)
                        # print(mycursor.statement)

        mydb.commit()

    print('- Persons committed')


def fetch_film(film_id):
    select_film_SQL = 'SELECT films.*, now()-films.updated AS last_update_sec FROM films WHERE id = %(film_id)s;'
    film_cursor = mydb.cursor(dictionary=True)
    film_cursor.execute(select_film_SQL, {'film_id': film_id})
    myresult = film_cursor.fetchone()
    # print(myresult)
    # print(myresult['last_update_sec'])

    if myresult['last_update_sec'] < 15 * 60:
        # print('{title_original} is fresh ({last_update_sec} sec old) in our records'.format(**myresult))
        return myresult

    # print('Fetching {title_eng} [{id}]'.format(**myresult))
    root_path = 'film'.split('.')
    userUrl = 'https://eventival.eu/poff/23/en/ws/VYyOdFh8AFs6XBr7Ch30tu12FljKqS/films/{film_id}.xml'.format(film_id=film_id)
    # json_fn = film['json']

    with urlopen_with_retry(userUrl) as url:
        data = url.read()
    XML_data = data.decode()
    dd = xmltodict.parse(XML_data)
    for elem in root_path:
        dd = dd[elem]
    # with open(json_fn, 'w') as json_file:
    #     json.dump(dd, json_file, indent=4)
    # print ('Done with ' + json_fn)

    SQL = """INSERT IGNORE INTO films (id, updated,
            title_est, title_original,
            runtime, year, premiere_type, trailer_url,
            directors_bio_est, directors_bio_eng, directors_bio_rus,
            synopsis_est, synopsis_eng, synopsis_rus,
            directors_filmography_est, directors_filmography_eng, directors_filmography_rus)
        VALUES (%(id)s, now(),
            %(title_est)s, %(title_original)s,
            %(runtime)s, %(year)s, %(premiere_type)s, %(trailer_url)s,
            %(directors_bio_est)s, %(directors_bio_eng)s, %(directors_bio_rus)s,
            %(synopsis_est)s, %(synopsis_eng)s, %(synopsis_rus)s,
            %(directors_filmography_est)s, %(directors_filmography_eng)s, %(directors_filmography_rus)s)
        ON DUPLICATE KEY UPDATE
            updated=now(),
            title_est=%(title_est)s, title_original=%(title_original)s,
            runtime=%(runtime)s, year=%(year)s, premiere_type=%(premiere_type)s, trailer_url=%(trailer_url)s,
            directors_bio_est=%(directors_bio_est)s, directors_bio_eng=%(directors_bio_eng)s, directors_bio_rus=%(directors_bio_rus)s,
            synopsis_est=%(synopsis_est)s, synopsis_eng=%(synopsis_eng)s, synopsis_rus=%(synopsis_rus)s,
            directors_filmography_est=%(directors_filmography_est)s, directors_filmography_eng=%(directors_filmography_eng)s, directors_filmography_rus=%(directors_filmography_rus)s
        ;"""

    map = {'id':dd['ids']['system_id'].get('#text'),
        'title_est':BeautifulSoup(dd['titles']['title_local'].get('#text') or '', features="html.parser").get_text().strip(),
        'title_original':BeautifulSoup(dd['titles']['title_original'].get('#text') or '', features="html.parser").get_text().strip(),
        'runtime':dd['film_info']['runtime']['seconds'],
        'year':dd['film_info']['completion_date']['year'] or '',
        'premiere_type':dd['film_info']['premiere_type'].get('#text') or '',
        'trailer_url':dd['film_info']['online_trailer_url'].get('#text') or '',
        'directors_bio_est':BeautifulSoup(dd['publications'].get('et',{}).get('directors_bio') or '', features="html.parser").get_text().strip(),
        'directors_bio_eng':BeautifulSoup(dd['publications'].get('en',{}).get('directors_bio') or '', features="html.parser").get_text().strip(),
        'directors_bio_rus':BeautifulSoup(dd['publications'].get('ru',{}).get('directors_bio') or '', features="html.parser").get_text().strip(),
        'synopsis_est':BeautifulSoup(dd['publications'].get('et',{}).get('synopsis_long') or '', features="html.parser").get_text().strip(),
        'synopsis_eng':BeautifulSoup(dd['publications'].get('en',{}).get('synopsis_long') or '', features="html.parser").get_text().strip(),
        'synopsis_rus':BeautifulSoup(dd['publications'].get('ru',{}).get('synopsis_long') or '', features="html.parser").get_text().strip(),
        'directors_filmography_est':BeautifulSoup(dd['publications'].get('et',{}).get('directors_filmography') or '', features="html.parser").get_text().strip(),
        'directors_filmography_eng':BeautifulSoup(dd['publications'].get('en',{}).get('directors_filmography') or '', features="html.parser").get_text().strip(),
        'directors_filmography_rus':BeautifulSoup(dd['publications'].get('ru',{}).get('directors_filmography') or '', features="html.parser").get_text().strip(),
    }

    film_cursor.execute(SQL, map)
    mydb.commit()

    film_cursor.execute(select_film_SQL, {'film_id': film_id})
    myresult = film_cursor.fetchone()

    # print('{title_original} is updated ({last_update_sec} sec old) in our records'.format(**myresult))
    return myresult


for subfest in subfests:
    # print('subfest:', subfest)
    fetch_base(subfest)
