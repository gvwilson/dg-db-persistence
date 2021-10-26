#!/usr/bin/env python

'''Checking types wiht Pydantic.'''

import sys
from typing import Any
from pydantic import BaseModel


class PotionDetails(BaseModel):
    potion: str

    def __repr__(self):
        return f'<PotionDetails potion="{self.potion}">'


class SpellDetails(BaseModel):
    spell: int

    def __repr__(self):
        return f'<SpellDetails spell={self.spell}>'


class Experiment(BaseModel):
    name: str
    details: Any

    def __repr__(self):
        return f'<Experiment name="{self.name}" details={self.details}>'


print('Pydantic only')
print('..', Experiment(name='with potion', details=PotionDetails(potion='syrup')))
print('..', Experiment(name='with potion', details=SpellDetails(spell=1234)))
