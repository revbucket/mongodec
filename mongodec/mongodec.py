#@ignore_file
""" Authentication and host information for connecting to dashboard database """

from pymongo import MongoClient
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
#                               GENERIC CHANGELING CLASS                      #
#                                                                             #
###############################################################################
'''
class Changeling(object):
    """ Generic class that serves as a parent for all our imitation classes.
        This and all its children are instatiated as an object and will allow
        one to easily overwrite any method, but maintain standard behavior for
        undecorated methods.
    """
    def __init__(self, base_object, cdict=None):
        self.base_object = base_object
        self.no_wrap_all = False
        self.cdict = cdict or {}
        self.class_prefix = self.base_object.__class__.__name__

    def __getattr__(self, name):
        methods = self.class_prefix + '_methods'
        if not callable(getattr(self.base_object, name)):
            return getattr(self.base_object, name)

        elif self.cdict.get(methods, {}).get(name) is not None:
            func = self.cdict[methods][name]
            def wrapper(*args, **kwargs):
                if kwargs.pop('no_changeling', False):
                    return getattr(self.base_object, name)(*args, **kwargs)
                callargs = convert_arg_soup(getattr(self.base_object, name),
                                            *args, **kwargs)
                return func(getattr(self.base_object, name), cdict=self.cdict,
                            callargs=callargs)

        else:
            def wrapper(*args, **kwargs):
                return getattr(self.base_object, name)(*args, **kwargs)

        if (self.cdict.get(self.class_prefix + '_wrap_all') is not None and
            not self.no_wrap_all):
            wrap_all = self.cdict[self.class_prefix + '_wrap_all']
            def final_wrapper(*args, **kwargs):
                if kwargs.get('no_changeling'):
                    return wrapper(*args, **kwargs)
                else:
                    callargs = convert_arg_soup(getattr(self.base_object, name),
                                                *args, **kwargs)
                    return wrap_all(wrapper, self.cdict, callargs)
        else:
            final_wrapper = wrapper

        return final_wrapper


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



##############################################################################
#                                                                            #
#                           FILTER MONGO STUFF                               #
#                                                                            #
##############################################################################

class FilterMongoDB(Changeling):
    """ Wrapper for mongoDB object.
        Supports accessing collections using the .property or the ['indexing']
        accessors. Returns ChangelingCollections everywhere
    """
    def __init__(self, base_object, _filter=None):
        super(self.__class__, self).__init__(base_object)
        self._filter = _filter

    def __getattr__(self, name):
        if isinstance(getattr(self.base_object, name), Collection):
            collection_obj = self.base_object[name]
            return FilterCollection(collection_obj, _filter=self._filter)
        elif name in ['create_collection', 'get_collection']:
            def wrapper(*args, **kwargs):
                collection_obj = getattr(self.base_object, name)(*args,
                                                                 **kwargs)
                return FilterCollection(collection_obj, _filter=self._filter)
            return wrapper
        else:
            return super(self.__class__, self).__getattr__(name)

    def __getitem__(self, collection_name):
        collection_obj = self.base_object[collection_name]
        return FilterCollection(collection_obj, _filter=self._filter)



class FilterCollection(Changeling):

    def __init__(self, base_object, _filter=None):
        super(self.__class__, self).__init__(base_object)
        self._filter = _filter

        method_dict = {}
        self.cdict['%s_methods' % self.class_prefix] = method_dict
        self.cdict['update_filter'] = _filter

        for method in ['count', 'replace_one', 'update_one', 'update_many',
                       'delete_one', 'delete_many',
                       'find_one_and_delete', 'find_one_and_replace',
                       'find_one_and_update', 'distinct']:

            method_dict[method] = replace_arg('filter', update_filter,
                                              cdict=self.cdict)

            method_dict['update'] = replace_arg('spec', update_filter,
                                                cdict=self.cdict)
            method_dict['remove'] = replace_arg('spec_or_id',
                                                update_filter,
                                                cdict=self.cdict)
            method_dict['aggregate'] = replace_arg('pipeline',
                                                   modify_agg_pipeline,
                                                   cdict=self.cdict)
            method_dict['group'] = replace_arg('condition', update_filter,
                                               cdict=self.cdict)
        self.cdict['%s_wrap_all' % self.class_prefix] = mongo_timeout_wrap

    ######################################################################
    #   Wrappers and weird overwrite methods                             #
    ######################################################################

    def find(self, _filter=None, projection=None, no_changeling=False,
             **other_kwargs):
        """ Not handled by the getattr because the implementation doesn't name
            args past *args, **kwargs
        """
        if not no_changeling:
            _filter = update_filter('filter', self.cdict,
                                    {'filter': _filter})['filter']

        return self.base_object.find(_filter, projection, **other_kwargs)


    def find_one(self, _filter=None, projection=None, no_changeling=False,
                 **other_kwargs):
        """ Not handled by the getattr because the implementation doesn't name
            args past *args, **kwargs
        """
        if not no_changeling:
            _filter = update_filter('filter', self.cdict,
                                    {'filter': _filter})['filter']

        return self.base_object.find_one(_filter, projection, **other_kwargs)



    def initialize_unordered_bulk_op(self, **kwargs):
        """ Builds a changeling BulkOperationBuilder instance
        See docs http://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.initialize_unordered_bulk_op
        for more details
        """
        bulk_op = self.base_object.initialize_unordered_bulk_op(**kwargs)
        return FilterMongoBulkOperationBuilder(bulk_op, _filter=self._filter)


    def initialize_ordered_bulk_op(self, **kwargs):
        """ Builds a changeling BulkOperationBuilder instance
        See docs http://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.initialize_unordered_bulk_op
        for more details
        """
        bulk_op = self.base_object.initialize_ordered_bulk_op(**kwargs)
        return FilterMongoBulkOperationBuilder(bulk_op, _filter=self._filter)




class FilterMongoBulkOperationBuilder(Changeling):
    def __init__(self, base_object, _filter=None):
        super(self.__class__, self).__init__(base_object)
        self._filter = _filter
        self.no_wrap_all = True


    def find(self, selector, no_changeling=False, **other_kwargs):
        if not no_changeling:
            selector = update_filter('selector',
                                     {'update_filter': self._filter},
                                     {'selector': selector})['selector']

        return self.base_object.find(selector, **other_kwargs)




'''
##############################################################################
#                                                                            #
#                               HELPER FUNCTIONS                             #
#                                                                            #
##############################################################################
'''

def convert_arg_soup(function, *args, **kwargs):
    """ Takes a function and it's given args and kwargs and makes them all
        kwargs.
    ARGS:
        function - a function that can be called like
                   function(*args, **kwargs)
        *args - args given to the function
        **kwargs - kwargs given to the function
    RETURNS:
        a dict of kwargs that can then be called like
        function(**RETURNVALUE)
    """
    callargs = inspect.getcallargs(function, *args, **kwargs)
    if 'kwargs' in callargs:
        callargs.update(callargs.pop('kwargs'))

    if 'self' in callargs:
        del callargs['self']

    return callargs


def replace_arg(argname, replacer, cdict=None):
    def wrapper(wrappee, callargs, cdict=cdict):
        return wrappee(**replacer(argname, cdict=cdict, callargs=callargs))
    return wrapper


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


def update_filter_direct(_filter, filter_kwargs):
    pass

