#!/usr/bin/env python

"""
Step 3: custom serializer. In practice, we would give the original
classes multiple parent classes, and later on we'll rely on the fact
that Pydantic identifies fields for encoding rather than listing them
explicitly with `_enc`.
"""

import json
import util
from test01_mem_only import \
  Experiment as BaseExperiment, \
  DetailsTxt as BaseDetailsTxt, \
  DetailsNum as BaseDetailsNum
from util import JSON_CLS, show

class Encodable:
    '''Empty base class to make encodable classes identifiable.'''
    pass

class Experiment(BaseExperiment, Encodable):
    _enc = ['name', 'details']

class DetailsTxt(BaseDetailsTxt, Encodable):
    _enc = ['text']

class DetailsNum(BaseDetailsNum, Encodable):
    _enc = ['number']

class Encoder(json.JSONEncoder):
    '''Custom JSON encoder.'''

    def default(self, obj):
        '''Record encodable objects as dicts with class name.'''
        if isinstance(obj, Encodable):
            class_name = obj.__class__.__name__
            obj = {key:obj.__dict__[key] for key in obj._enc}
            obj[JSON_CLS] = class_name
        return obj


tests = [
    Experiment('with dictionary', {'k': 0}),
    Experiment('with text', DetailsTxt('text content')),
    Experiment('with number', DetailsNum(1234))
]
print('== Custom JSON encoding.')
show('encodable test cases', tests)
show('encoded JSON persistence',
     [json.dumps(e, cls=Encoder) for e in tests])
