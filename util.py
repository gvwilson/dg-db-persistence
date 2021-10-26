import sys

# Special field encoding class name for JSON persistence.
JSON_CLS = '_json_cls'

def show(title, values):
    '''Show a list of values.'''
    print(title)
    for v in values:
        print('..', v)
