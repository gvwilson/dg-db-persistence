#!/usr/bin/env python

'''Checking types wiht Pydantic.'''

import sys
from typing import Type, Any
from pydantic import BaseModel


class BaseDetails(BaseModel):
    pass


class PotionDetails(BaseDetails):
    potion: str

    def __repr__(self):
        return f'<PotionDetails potion="{self.potion}">'


class SpellDetails(BaseDetails):
    spell: int

    def __repr__(self):
        return f'<SpellDetails spell={self.spell}>'


class Experiment(BaseModel):
    name: str
    details: Type[BaseDetails]

    def __repr__(self):
        return f'<Experiment name="{self.name}" details={self.details}>'


print('Pydantic with base classes fails')
try:
    print('..', Experiment(name='with potion', details=PotionDetails(potion='syrup')))
    print('..', Experiment(name='with potion', details=SpellDetails(spell=1234)))
except Exception as e:
    print('Exception:', str(e))

print('Pydantic with base classes constructed from dicts also fails')
try:
    print('..', Experiment(name='with potion', details={'potion': 'syrup'}))
    print('..', Experiment(name='with potion', details={'spell': 1234}))
except Exception as e:
    print('Exception:', str(e))
