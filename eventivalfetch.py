import os, sys
import urllib.request, json, csv

import time
from functools import wraps

import xmltodict

from bs4 import BeautifulSoup

interesting_film_id = None # '528663'

ANXIETY = 15 * 60; # time in seconds that will make a film anxious and willing to look for updates

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

# print(db)

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
    1838: 'Shortsi alam',
    2651: 'KinOFF',
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
        # 'url': 'https://eventival.eu/poff/23/en/ws/VYyOdFh8AFs6XBr7Ch30tu12FljKqS/films/publications-locked.xml',
        'url': 'https://eventival.eu/poff/23/en/ws/VYyOdFh8AFs6XBr7Ch30tu12FljKqS/films/categories/{subfest}/publications-locked.xml',
        'json': 'publications.json',
        'root_path': 'films.item'
    },
    'screenings' : {
        # 'url': 'https://eventival.eu/poff/23/en/ws/VYyOdFh8AFs6XBr7Ch30tu12FljKqS/films/screenings.xml',
        'url': 'https://eventival.eu/poff/23/en/ws/VYyOdFh8AFs6XBr7Ch30tu12FljKqS/films/categories/{subfest}/screenings.xml',
        'json': 'screenings.json',
        'root_path': 'screenings.screening'
    }
}

def clean_empty(d, needle):
    if not isinstance(d, (dict, list)):
        return d
    if isinstance(d, list):
        return [v for v in (clean_empty(v, needle) for v in d) if v]
    return {k: v for k, v in ((k, clean_empty(v, needle)) for k, v in d.items()) if v and k != needle}

def mySoap(text):
    if not text:
        return ''
    text = text.replace('</p>', '||BR||').replace('<br>', '||BR||').replace('<br/>', '||BR||').replace('<br />', '||BR||')
    paragraphs = text.split('||BR||')
    paragraphs = [BeautifulSoup(p, features="html.parser").get_text().strip() for p in paragraphs]
    paragraphs = filter(None, paragraphs)
    text = '\n'.join(paragraphs)
    paragraphs = text.split('\n')
    paragraphs = [p.strip() for p in paragraphs]
    paragraphs = filter(None, paragraphs)
    return "<p>" + "</p>\n<p>".join(paragraphs) + "</p>"


def fetch_base(subfest):
# def fetch_base():
    for task in tasks:
        root_path = tasks[task]['root_path'].split('.')
        userUrl = tasks[task]['url'].format(subfest=subfest)
        # userUrl = tasks[task]['url']
        json_fn = os.path.join(datadir, str(subfest) + '_' + tasks[task]['json'])
        print ('Fetch ' + userUrl + ' to ' + json_fn)

        with urlopen_with_retry(userUrl) as url:
            data = url.read()
            # print('Got {len} bytes worth of HTTP data'.format(len=len(data)))
        XML_data = data.decode()
        # print('Got {len} bytes worth of XML_data'.format(len=len(XML_data)))

        dict_data = clean_empty(xmltodict.parse(XML_data), '')
        for elem in root_path:
            dict_data = dict_data.get(elem,{})
        dict_data = clean_empty(dict_data, 'hash')

        if dict_data == {}:
            print('#### Got just {len} bytes worth of JSON'.format(len=len(json.dumps(dict_data))))
            return

        with open(json_fn, 'w') as json_file:
            json.dump(dict_data, json_file, indent=4)
            # print ('Done with ' + json_fn)

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
                # if elem in item:
                    # value = item[elem]
                value = item.get(elem,{})
                # print('value:', value)
                for elem in path:
                    value = value.get(elem,{})
                    # if value and elem in value:
                # print('map:', mapping, '<-', mappings[mapping], ' = ', value)
                if value == {}:
                    value = None;
                map[mapping] = value
            if SQL:
                # print(map)
                mycursor.execute(SQL, map)
                # print(mycursor.statement)
        # print('commit')
        mydb.commit()


film_counter = 1
def parse_publications(dict_data, task):
    print('Parse ' + task)
    global film_counter
    # print('dd', dict_data)
    if not isinstance(dict_data, list):
        dict_data = [dict_data]
        # print('is list?', isinstance(dict_data, list))
    # return

    SQL = """INSERT IGNORE INTO films (id, title_eng, title_original, published)
        VALUES (%(id)s, %(title_eng)s, %(title_original)s, subtime(now(),SEC_TO_TIME(86400)))
        ON DUPLICATE KEY UPDATE
        title_eng=%(title_eng)s, title_original=%(title_original)s
    ;"""

    for item in dict_data:
        if interesting_film_id and interesting_film_id > item['id']:
            print('skip', item['id'], '>', interesting_film_id)
            continue
        print(film_counter, 'Film', item['id'], item.get('title_english', 'WARNING, Film has no title_english.          *** *** *** *** ***'))
        map = { 'id': item['id'],
                'title_eng': item.get('title_english'),
                'title_original': item.get('title_original'),
                'anxiety': ANXIETY
              }
        mycursor.execute(SQL, map)
        # print(mycursor.statement)

        fetch_film(item['id'])
        mydb.commit()
        film_counter += 1

    print('- {film_counter} films committed'.format(film_counter=film_counter))


    # filmFestival / eventival_categorization -> categories -> category 
    SQLs = [
        """INSERT IGNORE INTO c_poffFest (id, est)
        VALUES (%(id)s, %(est)s)
        ;""",
        """INSERT IGNORE INTO film_poffFest (film_id, poffFest_id)
        VALUES (%(film_id)s, %(id)s)
        ;"""
    ]
    for item in dict_data:
        try:
            festivals = item['eventival_categorization']['categories']['category']
        except Exception as e:
            print('No festivals, skipping ', item)
            continue
        if not isinstance(festivals, list):
            festivals = [festivals]
        for festival in festivals:
            map = { 'id': festival['@id'], 'est': festival['#text'], 'film_id': item['id'] }
            for SQL in SQLs:
                mycursor.execute(SQL, map)
                # print(mycursor.statement)
        mydb.commit()
    print('- Festivals committed')


    # filmProgram / eventival_categorization -> sections -> section 
    SQLs = [
        """INSERT IGNORE INTO c_program (id, est)
        VALUES (%(id)s, %(est)s)
        ON DUPLICATE KEY UPDATE
        est=%(est)s
        ;""",
        """INSERT IGNORE INTO film_programs (film_id, program_id)
        VALUES (%(film_id)s, %(id)s)
        ;"""
        ]
    for item in dict_data:
        # try:
        programs = item['eventival_categorization'].get('sections',{}).get('section',[])
        # except Exception as e:
        #     print('No sections, skipping ', item)
        #     continue

        if not isinstance(programs, list):
            programs = [programs]
        for program in programs:
            map = { 'id': program['id'], 'est': program['name'], 'film_id': item['id'] }
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

    SQL = """INSERT IGNORE INTO screenings ( id
        , screening_code, film_id, cinema_hall_id, venue_id
        , start_date, start_time, ticketing_url
        , screening_duration_minutes, presentation_duration_minutes, qa_duration_minutes
        , screening_info_est, screening_info_eng, screening_info_rus)
        VALUES ( %(id)s
        , %(screening_code)s, %(film_id)s, %(cinema_hall_id)s, %(venue_id)s
        , %(start_date)s, %(start_time)s, %(ticketing_url)s
        , %(screening_duration_minutes)s, %(presentation_duration_minutes)s, %(qa_duration_minutes)s
        , %(screening_info_est)s, %(screening_info_eng)s, %(screening_info_rus)s)
        ON DUPLICATE KEY UPDATE screening_code=%(screening_code)s
        , film_id=%(film_id)s, cinema_hall_id=%(cinema_hall_id)s, venue_id=%(venue_id)s
        , start_date=%(start_date)s, start_time=%(start_time)s, ticketing_url=%(ticketing_url)s
        , screening_duration_minutes=%(screening_duration_minutes)s
        , presentation_duration_minutes=%(presentation_duration_minutes)s
        , qa_duration_minutes=%(qa_duration_minutes)s
        , screening_info_est=%(screening_info_est)s, screening_info_eng=%(screening_info_eng)s, screening_info_rus=%(screening_info_rus)s
    ;"""
    i = 0
    for item in dict_data:
        i+=1
        # continue
        map = { 'id': item['id']
              , 'screening_code': item.get('code'), 'film_id': item['film']['id'], 'cinema_hall_id': item.get('cinema_hall_id'), 'venue_id': item['venue_id']
              , 'start_date': item['start'][:10], 'start_time': item['start'][11:], 'ticketing_url': item.get('ticketing_url')
              , 'screening_duration_minutes': item['duration_screening_only_minutes']
              , 'presentation_duration_minutes': item.get('presentation',{}).get('duration')
              , 'qa_duration_minutes': item['qa'].get('duration')
              , 'screening_info_est': item.get('additional_info',{}).get('et')
              , 'screening_info_eng': item.get('additional_info',{}).get('en')
              , 'screening_info_rus': item.get('additional_info',{}).get('ru')
              }
        mycursor.execute(SQL, map)
        # print(i, mycursor.statement)

        # screeningType / type_of_screening
        screeningSQLs = [
        # "INSERT IGNORE INTO c_screeningType (code, est) VALUES (%(est)s, %(est)s);",
        "UPDATE screenings SET type_of_screening = %(type_of_screening)s WHERE id = %(id)s;"
        ]
        map = { 'id': item['id'], 'type_of_screening': item.get('type_of_screening', 'regular') }
        for screeningSQL in screeningSQLs:
            # print('got presenter for presentation for screening', screeningSQL, map)
            mycursor.execute(screeningSQL, map)
            # print(mycursor.statement)

        # Film Languages
        flSQL = 'INSERT IGNORE INTO screening_film_languages (screening_id, language_code) VALUES (%(id)s, %(ISOLanguage)s);'
        ISOLanguages = item.get('film',{}).get('languages',{}).get('print',{}).get('language',[])
        if not isinstance(ISOLanguages, list):
            ISOLanguages = [ISOLanguages]
        for ISOLanguage in ISOLanguages:
            map = { 'id':item['id'],
                    'ISOLanguage':ISOLanguage }
            mycursor.execute(flSQL, map)

        # Subtitle Languages
        slSQL = 'INSERT IGNORE INTO screening_subtitle_languages (screening_id, language_code) VALUES (%(id)s, %(ISOLanguage)s);'
        ISOLanguages = item.get('film',{}).get('subtitle_languages',{}).get('print',{}).get('language',[])
        if not isinstance(ISOLanguages, list):
            ISOLanguages = [ISOLanguages]
        for ISOLanguage in ISOLanguages:
            map = { 'id':item['id'],
                    'ISOLanguage':ISOLanguage }
            mycursor.execute(slSQL, map)



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
        if item['presentation'].get('presenters'):
            (part, role) = ('presentation', 'presenter')
            presenters = item['presentation'].get('presenters',{}).get('person')
            if not isinstance(presenters, list):
                presenters = [presenters]
            for presenter in presenters:
                relations = presenter.get('relations',{}).get('relation')
                if not isinstance(relations, list):
                    relations = [relations]
                for relation in relations:
                    map = { 'person_id': presenter['@id'], 'person_name': presenter['name'],
                        'screening_id': item['id'], 'part': part, 'role': role, 'relation_name': relation }
                    for SQL in SQLs:
                        # print('got presenter for presentation for screening', SQL, map)
                        mycursor.execute(SQL, map)
                        # print(mycursor.statement)
        if item['presentation'].get('guests'):
            (part, role) = ('presentation', 'guest')
            guests = item['presentation'].get('guests',{}).get('person')
            if not isinstance(guests, list):
                guests = [guests]
            for guest in guests:
                relations = guest.get('relations',{}).get('relation')
                if not isinstance(relations, list):
                    relations = [relations]
                for relation in relations:
                    map = { 'person_id': guest['@id'], 'person_name': guest['name'],
                        'screening_id': item['id'], 'part': part, 'role': role, 'relation_name': relation }
                    for SQL in SQLs:
                        # print('got guest for presentation for screening', SQL, map)
                        mycursor.execute(SQL, map)
                        # print(mycursor.statement)
        if item['qa'].get('presenters'):
            (part, role) = ('qa', 'presenter')
            presenters = item['qa'].get('presenters',{}).get('person')
            if not isinstance(presenters, list):
                presenters = [presenters]
            for presenter in presenters:
                relations = presenter.get('relations',{}).get('relation')
                if not isinstance(relations, list):
                    relations = [relations]
                for relation in relations:
                    map = { 'person_id': presenter['@id'], 'person_name': presenter['name'],
                        'screening_id': item['id'], 'part': part, 'role': role, 'relation_name': relation }
                    for SQL in SQLs:
                        # print('got presenter for qa for screening', SQL, map)
                        mycursor.execute(SQL, map)
                        # print(mycursor.statement)
        if item['qa'].get('guests'):
            (part, role) = ('qa', 'guest')
            guests = item['qa'].get('guests',{}).get('person')
            if not isinstance(guests, list):
                guests = [guests]
            for guest in guests:
                relations = guest.get('relations',{}).get('relation')
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

    if not myresult:
        myresult = {}
    # if myresult.get('last_update_sec',0) < ANXIETY:
    #     return myresult

    root_path = 'film'.split('.')
    userUrl = 'https://eventival.eu/poff/23/en/ws/VYyOdFh8AFs6XBr7Ch30tu12FljKqS/films/{film_id}.xml'.format(film_id=film_id)
    myresult['userUrl'] = userUrl
    # print('Fetching {title_eng} [{id}] from {userUrl}'.format(**myresult))

    with urlopen_with_retry(userUrl) as url:
        data = url.read()
    XML_data = data.decode()
    dd = xmltodict.parse(XML_data)
    for elem in root_path:
        dd = dd[elem]
    json_fn = os.path.join(datadir, 'films', '{id}.json'.format(id=myresult['id']))
    with open(json_fn, 'w') as json_file:
        json.dump(clean_empty(dd, '@label'), json_file, indent=4)
    # print ('Done with ' + json_fn)

    SQL = """INSERT IGNORE INTO films (id, updated,
            title_est, title_eng, title_rus, title_original,
            runtime, year, premiere_type, trailer_url,
            directors_bio_est, directors_bio_eng, directors_bio_rus,
            synopsis_est, synopsis_eng, synopsis_rus,
            directors,
            producers,
            writers,
            cast,
            DoP,
            editors,
            music,
            production,
            distributors,
            festivals_est, festivals_eng, festivals_rus,
            directors_filmography_est, directors_filmography_eng, directors_filmography_rus)
        VALUES (%(id)s, now(),
            %(title_est)s, %(title_eng)s, %(title_rus)s, %(title_original)s,
            %(runtime)s, %(year)s, %(premiere_type)s, %(trailer_url)s,
            %(directors_bio_est)s, %(directors_bio_eng)s, %(directors_bio_rus)s,
            %(synopsis_est)s, %(synopsis_eng)s, %(synopsis_rus)s,
            %(directors)s,
            %(producers)s,
            %(writers)s,
            %(cast)s,
            %(DoP)s,
            %(editors)s,
            %(music)s,
            %(production)s,
            %(distributors)s,
            %(festivals_est)s, %(festivals_eng)s, %(festivals_rus)s,
            %(directors_filmography_est)s, %(directors_filmography_eng)s, %(directors_filmography_rus)s)
        ON DUPLICATE KEY UPDATE
            updated = now(),
            title_est = %(title_est)s, title_eng = %(title_eng)s, title_rus = %(title_rus)s, title_original = %(title_original)s,
            runtime = %(runtime)s, year = %(year)s, premiere_type = %(premiere_type)s, trailer_url = %(trailer_url)s,
            directors_bio_est = %(directors_bio_est)s, directors_bio_eng = %(directors_bio_eng)s, directors_bio_rus = %(directors_bio_rus)s,
            synopsis_est = %(synopsis_est)s, synopsis_eng = %(synopsis_eng)s, synopsis_rus = %(synopsis_rus)s,
            directors = %(directors)s,
            producers = %(producers)s,
            writers = %(writers)s,
            cast = %(cast)s,
            DoP = %(DoP)s,
            editors = %(editors)s,
            music = %(music)s,
            production = %(production)s,
            distributors = %(distributors)s,
            festivals_est = %(festivals_est)s, festivals_eng = %(festivals_eng)s, festivals_rus = %(festivals_rus)s,
            directors_filmography_est = %(directors_filmography_est)s, directors_filmography_eng = %(directors_filmography_eng)s, directors_filmography_rus = %(directors_filmography_rus)s
        ;"""

    def getCrew(crew_a, type):
        for crew in crew_a:
            if crew['type']['name'] == type:
                return BeautifulSoup(crew.get('text') or '', features="html.parser").get_text().strip()



    map = {'id':         dd['ids']['system_id'].get('#text'),
        'runtime':       dd['film_info']['runtime']['seconds'],
        'year':          dd['film_info']['completion_date']['year']         or '',
        'premiere_type': dd['film_info']['premiere_type'].get('#text')      or '',
        'trailer_url':   dd['film_info']['online_trailer_url'].get('#text') or '',
        'directors':                   BeautifulSoup(dd['publications'].get('en',{}).get('directors')             or '', features="html.parser").get_text().strip(),
        'producers':                   BeautifulSoup(dd['publications'].get('en',{}).get('producers')             or '', features="html.parser").get_text().strip(),
        'writers':                     BeautifulSoup(dd['publications'].get('en',{}).get('writers')               or '', features="html.parser").get_text().strip(),
        'cast':                        BeautifulSoup(dd['publications'].get('en',{}).get('cast')                  or '', features="html.parser").get_text().strip(),

        'DoP':                         getCrew(dd['publications'].get('en',{}).get('crew',{}).get('contact',{}), 'Op/DoP'),
        'editors':                     getCrew(dd['publications'].get('en',{}).get('crew',{}).get('contact',{}), 'Mont/Ed'),
        'music':                       getCrew(dd['publications'].get('en',{}).get('crew',{}).get('contact',{}), 'Muusika/Music'),
        'production':                  getCrew(dd['publications'].get('en',{}).get('crew',{}).get('contact',{}), 'Tootja/Production'),
        'distributors':                getCrew(dd['publications'].get('en',{}).get('crew',{}).get('contact',{}), 'Levitaja/Distributor'),

        'title_original':              BeautifulSoup(dd['titles']['title_original'].get('#text')                  or '', features="html.parser").get_text().strip(),
        'title_est':                   BeautifulSoup(dd['titles']['title_local'].get('#text')                     or '', features="html.parser").get_text().strip(),
        'title_eng':                   BeautifulSoup(dd['titles']['title_english'].get('#text')                   or '', features="html.parser").get_text().strip(),
        'synopsis_est':                mySoap(dd['publications'].get('et',{}).get('synopsis_long','')),
        'synopsis_eng':                mySoap(dd['publications'].get('en',{}).get('synopsis_long','')),
        'festivals_est':               BeautifulSoup(dd['publications'].get('et',{}).get('synopsis_short')        or '', features="html.parser").get_text().strip(),
        'festivals_eng':               BeautifulSoup(dd['publications'].get('en',{}).get('synopsis_short')        or '', features="html.parser").get_text().strip(),
        'directors_bio_est':           BeautifulSoup(dd['publications'].get('et',{}).get('directors_bio')         or '', features="html.parser").get_text().strip(),
        'directors_bio_eng':           BeautifulSoup(dd['publications'].get('en',{}).get('directors_bio')         or '', features="html.parser").get_text().strip(),
        'directors_filmography_est':   BeautifulSoup(dd['publications'].get('en',{}).get('directors_filmography') or '', features="html.parser").get_text().strip(),
        'directors_filmography_eng':   BeautifulSoup(dd['publications'].get('en',{}).get('directors_filmography') or '', features="html.parser").get_text().strip(),
    }
    map['title_rus'] =                 BeautifulSoup(dd['titles']['title_custom'].get('#text')                    or map['title_eng'], features="html.parser").get_text().strip()
    map['synopsis_rus'] =              mySoap(dd['publications'].get('ru',{}).get('synopsis_long',''))            or map['synopsis_eng']
    map['festivals_rus'] =             BeautifulSoup(dd['publications'].get('ru',{}).get('festivals')             or map['festivals_eng'], features="html.parser").get_text().strip()
    map['directors_bio_rus'] =         BeautifulSoup(dd['publications'].get('ru',{}).get('directors_bio')         or map['directors_bio_eng'], features="html.parser").get_text().strip()
    map['directors_filmography_rus'] = BeautifulSoup(dd['publications'].get('ru',{}).get('directors_filmography') or map['directors_filmography_eng'], features="html.parser").get_text().strip()

    film_cursor.execute(SQL, map)
    # print(film_cursor.statement)
    mydb.commit()


    # Countries
    SQL = 'INSERT IGNORE INTO film_countries (film_id, country_code) VALUES (%(id)s, %(ISOCountry)s);'
    ISOCountries = dd['film_info']['countries'].get('country',{})
    if not isinstance(ISOCountries, list):
        ISOCountries = [ISOCountries]
    for ISOCountry in ISOCountries:
        map = { 'id':dd['ids']['system_id'].get('#text'),
                'ISOCountry':ISOCountry.get('code') }
        film_cursor.execute(SQL, map)


    # Languages
    SQL = 'INSERT IGNORE INTO film_languages (film_id, language_code) VALUES (%(id)s, %(ISOLanguage)s);'
    if 'language' in dd['film_info']['languages']:
        ISOLanguages = dd['film_info']['languages']['language']
    else:
        ISOLanguages = []
    if not isinstance(ISOLanguages, list):
        ISOLanguages = [ISOLanguages]
    for ISOLanguage in ISOLanguages:
        map = { 'id':dd['ids']['system_id'].get('#text'),
                'ISOLanguage':ISOLanguage['code'] }
        film_cursor.execute(SQL, map)


    # filmType / film_info -> length_type
    SQLs = [
        """INSERT IGNORE INTO c_type (id, est)
        VALUES (%(id)s, %(est)s)
        ON DUPLICATE KEY UPDATE
        est=%(est)s
        ;""",
        """INSERT IGNORE INTO film_types (film_id, type_id)
        VALUES (%(film_id)s, %(id)s)
        ;"""
    ]

    # filmGenre / film_info -> types -> type
    SQLs = [
        """INSERT IGNORE INTO c_genre (est)
        VALUES (%(est)s)
        ;""",
        """INSERT IGNORE INTO film_genres (film_id, genre_est)
        VALUES (%(film_id)s, %(est)s)
        ;"""
    ]
    genres = dd['film_info'].get('types',{}).get('type',[])
    if not isinstance(genres, list):
        genres = [genres]
    map = { 'film_id':film_id }
    print(genres)
    for est in genres:
        map['est'] = est
        for SQL in SQLs:
            film_cursor.execute(SQL, map)


    # filmKeyword / film_info -> texts -> directors_statement
    SQLs = [
        """INSERT IGNORE INTO c_keyword (est)
        VALUES (%(est)s)
        ;""",
        """INSERT IGNORE INTO film_keywords (film_id, keyword_id)
        SELECT %(film_id)s, id FROM c_keyword WHERE est = %(est)s
        ;"""
    ]
    import pprint
    pp = pprint.PrettyPrinter(indent=4).pprint
    keywords = dd['film_info']['texts']['directors_statement'].get('#text','').strip(' ,').split(',')
    # print(keywords)
    keywords = [kw.strip() for kw in keywords]
    map = { 'film_id':film_id }
    for keyword in keywords:
        if keyword == '':
            continue
        map['est'] = keyword
        # pp(map)
        for SQL in SQLs:
            film_cursor.execute(SQL, map)


    # logline / film_info -> texts -> logline
    SQLs = [
        """INSERT IGNORE INTO film_cassette (cassette_id, film_id)
        VALUES (%(cassette_id)s, %(film_id)s)
        ;"""
    ]
    import pprint
    pp = pprint.PrettyPrinter(indent=4).pprint
    # pp(dd['film_info']['texts']['logline'].get('#text','').strip(' ,').split(','))
    logline = dd['film_info']['texts']['logline'].get('#text','').strip(' ,').split(',')
    # print(keywords)
    logline = [kw.strip() for kw in logline]
    map = { 'cassette_id':film_id }
    for film_id in logline:
        if film_id == '':
            continue
        map['film_id'] = film_id
        # pp = pprint.PrettyPrinter(indent=4).pprint
        # pp(map)
        for SQL in SQLs:
            film_cursor.execute(SQL, map)
            # print(film_cursor.statement)

    mydb.commit()


    film_cursor.execute(select_film_SQL, {'film_id': film_id})
    myresult = film_cursor.fetchone()

    # print('{title_original} is updated ({last_update_sec} sec old) in our records'.format(**myresult))
    return myresult


def truncate():
    SQLs = [
        """TRUNCATE TABLE film_cassette;"""
    ]
    for SQL in SQLs:
        trunc_cursor = mydb.cursor(dictionary=True)
        trunc_cursor.execute(SQL, map)
        mydb.commit()


for subfest in subfests:
    print('subfest:', subfest)
    fetch_base(subfest)
# fetch_base()
