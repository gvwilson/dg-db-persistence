#!/usr/bin/env python

'''Checking types wiht Pydantic.'''

from pydantic import BaseModel


class BaseDetails(BaseModel):
    pass


print('Pydantic with empty class works')
print('..', BaseDetails())
