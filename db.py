import logging
import sqlite3

global DB
DB = dict()

def connect():
    global DB
    c = sqlite3.connect('world_heritage_sites.db', check_same_thread=False)
    c.row_factory = sqlite3.Row
    DB['conn'] = c
    DB['cursor'] = c.cursor()
    logging.info('Connected to database')

def execute(sql, args=None):
    global DB
    if args is None:
        args_sequence = tuple()
    elif isinstance(args, list):
        args_sequence = tuple(args)
    else:
        args_sequence = args
    logging.info('SQL: {} Args: {}'.format(sql, args_sequence))
    return DB['cursor'].execute(sql, args_sequence)

def close():
    global DB

    DB['conn'].close()
