#@ignore_file
""" Authentication and host information for connecting to dashboard database """

from pymongo import MongoClient
from changeling import Changeling
#from utilities.database.db_config import Changeling
import os
import inspect
import time
from pymongo.collection import Collection
from pymongo.errors import NetworkTimeout, ConnectionFailure
import json


'''
###############################################################################
#                                                                             #
#                                 CUSTOM MONGO OBJECT                         #
#                                                                             #
###############################################################################
'''


class MongoConfig(object):
    """Object that stores the parameters to connect to mongo.
       You can either pass in the connection params as kwargs XOR
       Store them in a json string in an environment variable (in which case
       you'd pass the environment variable's name)

    """
    def __init__(self, user=None, password=None, host=None, port=None,
                 database=None, replica_set=None, environ_var=None):
        """This class is just a wrapper for the above parameters"""
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.replica_set = replica_set

        # OR PASS AN ENV VAR
        self.environ_var = environ_var

    def client(self):
        """Returns a pymongo MongoClient instance"""
        if self.environ_var is not None:
            config_dict = json.loads(os.environ.get(self.environ_var))
        else:
            config_dict = {'user': self.user,
                           'password': self.password,
                           'host': self.host,
                           'port': self.port,
                           'database': self.database,
                           'replica_set': self.replica_set}


        user = config_dict.get('user')
        password =config_dict.get('password')
        host =config_dict.get('host')
        port = config_dict.get('port')
        database =config_dict.get('database')
        replica_set =config_dict.get('replica_set', '')


        if user is not None:
            db_uri = 'mongodb://%s:%s@%s:%s/%s%s' % (user, password, host, port,
                                                     database, replica_set)
        else:
            db_uri = 'mongodb://%s:%s/%s' % (host, port, database)

        return MongoClient(db_uri)

    def db(self):
        """Returns a pymongo Database instance"""

        if self.environ_var is not None:
            database = json.loads(os.environ.get(self.environ_var))['database']
        else:
            database = self.database
        return self.client()[database]


'''
##############################################################################
#                                                                            #
#                               HELPER FUNCTIONS                             #
#                                                                            #
##############################################################################
'''



def mongo_timeout_wrap(func, cdict, callargs):
    """ Wrapper that tries failed commands until we hit 30 seconds """
    start_time = time.time()
    while True:
        if time.time() - start_time > 30:
            raise Exception("Mongo query timeout wrapped over 30 seconds")
        try:
            return func(**callargs)
        except NetworkTimeout:
            pass
        except ConnectionFailure:
            pass
            # I don't think we have to rebuild the connection here


def modify_agg_pipeline(argname, cdict, callargs):
    assert argname == 'pipeline'
    callargs['pipeline'] = ([{'$match': update_filter(argname, cdict,
                                                      {argname: None})[argname]}
                            ] + callargs['pipeline'])

    return callargs


def update_filter(argname, cdict, callargs):
    """ Updates a mongo query to include self.composer_id.
    If _filter is None, we just use the cdict's update_filter kwargs
    If _filter is a dict and doesn't include composer_id, we add it in
    If _filter is not a dict, we assume it's a spec for the _id

    Modifies the callargs argument, but also returns the new callargs
    """
    filter_kwargs = cdict.get('update_filter') or {}
    _filter = callargs[argname]
    for k, v in filter_kwargs.iteritems():
        if _filter is None:
            _filter = {k: v}
        elif isinstance(_filter, dict):
            _filter[k] = _filter.get(k, v)
        else:
            _filter = {'_id': _filter, k: v}
    callargs[argname] = _filter
    return callargs


