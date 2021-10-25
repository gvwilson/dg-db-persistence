#!/usr/bin/env python

'''Experiment with persisting experimental data.'''

def show(title, items):
    print(title)
    for i in items:
        print('..', i)

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

ex = [
    Experiment("with potion", PotionDetails("syrup")),
    Experiment("with spell", SpellDetails(1234))
]
show("raw", ex)
