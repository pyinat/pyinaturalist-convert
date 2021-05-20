import json
from io import StringIO

import tablib
from flatdict import FlatDict
from flatten_dict import flatten as flatten2
from flatten_json import flatten as flatten1
from pyinaturalist import *
from tablib import Dataset, import_set

r = get_observations(iconic_taxa='Aves', ident_user_id='jkcook', per_page=1)
d = Dataset(r['results'])
print(d.get_csv())

t = get_taxa_by_id(48460)
d = Dataset(t['results'])
print(d.get_csv())

r = get_users_autocomplete('jkcook')
u = [r['results'][0]]
d = Dataset(u)
d.get_csv()

d = import_set(json.dumps(u, default=str), 'json')
print(d.get_csv())

r = get_observations(iconic_taxa='Aves', ident_user_id='jkcook', per_page=1)
d2 = import_set(json.dumps(r['results'], default=str), 'json')
print(d2.get_csv())


o1 = flatten1(
    r['results'][0], separator='.', root_keys_to_ignore=['non_owner_ids', 'observation_photos']
)
print(o1)
d3 = import_set(json.dumps([o1], default=str), 'json')
print(d3.get_csv())

o2 = FlatDict(r['results'][0], delimiter='.')
print(o2)

r = get_observations(iconic_taxa='Aves', ident_user_id='jkcook', per_page=2)
r = simplify_observations(r)
o3 = flatten2(r['results'][0], reducer='dot')
o3 = [flatten2(i, reducer='dot') for i in r]
