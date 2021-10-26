#!/usr/bin/env python

'''Step 2: try to persist in-memory classes as JSON.'''

import json
from test01_mem_only import Experiment, DetailsTxt

print('== Failed attempt to persist in-memory classes as JSON.')
m = Experiment('with text', DetailsTxt('text content')),
try:
    m_as_text = json.dumps(m)
except TypeError as e:
    print('direct in-memory to JSON:', e)
