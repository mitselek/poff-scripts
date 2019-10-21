import yaml

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


translations = {}
for lang in ('et','en','ru'):
    with open(r'translate.{lang}.yaml'.format(lang=lang)) as file:
        translations[lang] = yaml.load(file)

translations = ExtendedDict(translations)

def strings(key, lang):
    path = '{lang}.{key}'.format(lang=lang, key=key).split('.')
    default = '[{key}]'.format(key=key)
    return translations.multi_level_get(path, default)
