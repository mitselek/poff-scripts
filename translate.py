import yaml
import os
import collections

class ExtendedDict(dict):
    """changes a normal dict into one where you can hand a list
    as first argument to .get() and it will do a recursive lookup
    result = x.get(['a', 'b', 'c'], default_val)
    """
    def multi_level_get(self, key, default=None):
        if not isinstance(key, list):
            return self.get(key, default)
        # assume that the key is a list of recursively accessible dicts
        def get_one_level(key_list, level, d):
            if level >= len(key_list):
                if level > len(key_list):
                    raise IndexError
                return d[key_list[level-1]]
            return get_one_level(key_list, level+1, d[key_list[level-1]])

        try:
            return get_one_level(key, 1, self)
        except KeyError:
            return default


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
mycursor = mydb.cursor()

translations = {}
for lang in ('et','en','ru'):
    with open(r'translate.{lang}.yaml'.format(lang=lang)) as file:
        translations[lang] = yaml.load(file)

def print_paths(p, d, l):
    if isinstance(d, collections.abc.Mapping):
        print('--> path: "{p}"'.format(p=p))
        for k in d:
            v = d[k]
            if k in ['one', 'multiple']:
                print('M', k, p, d[k])
                mycursor.execute(SQL, {'path': p, 'lang': l, 'singular': d[k], 'plural': d[k]})
                print(mycursor.statement)
            else:
                print_paths('{p}.{k}'.format(p=p, k=k), v, l)
        print('<-- path: "{p}"'.format(p=p))
    else:
        print('    {l} = {p} = {v}'.format(l=l, p=p, v=d))
        # mycursor.execute(SQL, {'path': p, 'lang': l, 'singular': d, 'plural': d})




SQL = """INSERT IGNORE INTO translations (path, lang, singular)
    VALUES (%(path)s, %(lang)s, %(singular)s)
    ON DUPLICATE KEY UPDATE
    plural = %(plural)s
    ;"""

for lang in ('et','en','ru'):
    for p in translations[lang]:
        print_paths(p, translations[lang][p], lang)
        mydb.commit()




translations = ExtendedDict(translations)

def strings(key, lang):
    path = '{lang}.{key}'.format(lang=lang, key=key).split('.')
    default = '[{key}]'.format(key=key)
    return translations.multi_level_get(path, default)
