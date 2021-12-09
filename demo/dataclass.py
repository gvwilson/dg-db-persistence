#!/usr/bin/env python

from dataclasses import asdict, dataclass

@dataclass
class Thing:
    top: str


def factory(pairs):
    print('FACTORY WITH', pairs)
    result = dict(pairs) | {'added': 'added'}
    return result


first = Thing(top='first thing')
print(first, '==', asdict(first, dict_factory=factory))
