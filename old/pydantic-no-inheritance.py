#!/usr/bin/env python

'''Checking types wiht Pydantic.'''

import sys
from typing import Type, Any, Union
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
    details: Union[PotionDetails, SpellDetails]

    def __repr__(self):
        return f'<Experiment name="{self.name}" details={self.details}>'


print('Pydantic with base classes fails')
print('..', Experiment(name='with potion', details=PotionDetails(potion='syrup')))
print('..', Experiment(name='with potion', details=SpellDetails(spell=1234)))
