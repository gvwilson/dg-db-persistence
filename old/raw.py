#!/usr/bin/env python

'''Raw classes, no persistence.'''

class Experiment:
    def __init__(self, name, details):
        self.name = name
        self.details = details

    def __repr__(self):
        return f'<Experiment name="{self.name}" details={self.details}>'


class PotionDetails:
    def __init__(self, potion):
        self.potion = potion

    def __repr__(self):
        return f'<PotionDetails potion="{self.potion}">'


class SpellDetails:
    def __init__(self, spell):
        self.spell = spell

    def __repr__(self):
        return f'<SpellDetails spell={self.spell}>'


print('raw classes')
print('..', Experiment('with potion', PotionDetails('syrup')))
print('..', Experiment('with spell', SpellDetails(1234)))


