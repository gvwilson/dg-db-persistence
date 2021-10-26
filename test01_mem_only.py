#!/usr/bin/env python

'''Step 1: the classes we want, in-memory only.'''

import util

class Experiment:
    '''An experiment has a name and some details.'''

    def __init__(self, name, details):
        self.name = name
        self.details = details

    def __str__(self):
        return f'<{self.__class__.__name__} name="{self.name}" details={self.details}>'

class DetailsTxt:
    '''Details with text only.'''

    def __init__(self, text):
        self.text = text

    def __str__(self):
        return f'<{self.__class__.__name__} text="{self.text}">'

class DetailsNum:
    '''Details with number only.'''

    def __init__(self, number):
        self.number = number

    def __str__(self):
        return f'<{self.__class__.__name__} number={self.number}>'

print('== In-memory tests')
util.show('in-memory', [
    Experiment('with text', DetailsTxt('text content')),
    Experiment('with number', DetailsNum(1234))
])
