#!/usr/bin/env python

'''First experiments with Pydantic.'''

from pydantic import BaseModel

class User(BaseModel):
    id: int
    name = 'Jane Doe'

user = User(id='123')
print(user)
print(user.dict())
print(user.json())
print('User schema', User.schema())
print('object schema', user.schema())
