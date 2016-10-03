""" Tests for mongodec.py """

import unittest
import mongodec as md
import os, json
from pymongo.errors import NetworkTimeout, ConnectionFailure


def get_local_mongo():
    return md.MongoConfig(user=None, password=None, database='local',
                          host='localhost', port=27017).db()

def drop_collections(mongo_db):
    for coll in mongo_db.collection_names():
        try:
            mongo_db.drop_collection(coll)
        except:
            pass

def clear_db_decorator(test_function):
    def wrapper(*args, **kwargs):
        mongo_db = get_local_mongo()
        drop_collections(mongo_db)
        test_function(*args, **kwargs)
        drop_collections(mongo_db)
    return wrapper





class TestMongodec(unittest.TestCase):

    def test_Changeling(self):
        """ Simple changeling test """
        # Test that we can make a simple dummy class
        class Foo(object):
            def __init__(inst, val, prop=None):
                inst.val = val
                inst.prop = prop

            def f(self, arg):
                return self.val + arg
        # and that this class works as expected
        foo_instance = Foo(20, prop='harambe died for our sins')
        self.assertEqual(foo_instance.f(12), 32)

        # now make simple wrapper for Foo.f
        def dummy_wrap(func, cdict, callargs):
            return func(arg=callargs['arg'] + 400)

        c_foo_instance = md.Changeling(foo_instance,
                                       cdict={'Foo_methods': {'f': dummy_wrap}})

        self.assertEqual(c_foo_instance.f(12), 432)
        self.assertEqual(c_foo_instance.f(12, no_changeling=True), 32)
        self.assertEqual(c_foo_instance.prop, 'harambe died for our sins')

        c_wrap_all = md.Changeling(foo_instance,
                                   cdict={'Foo_methods': {'f': dummy_wrap},
                                          'Foo_wrap_all': dummy_wrap})

        self.assertEqual(c_wrap_all.f(12), 832)
        self.assertEqual(c_wrap_all.f(12, no_changeling=True), 32)



    '''
    ##########################################################################
    #                                                                        #
    #                       TEST CUSTOM MONGO OBJECT                         #
    #                                                                        #
    ##########################################################################
    '''
    def test_MongoConfig_args(self):
        config = md.MongoConfig(user=None, password=None, database='local',
                                host='localhost', port=27017)

        mongo_db = config.db()
        foobar_collection = mongo_db['foobar']
        foobar_collection.remove()
        foobar_collection.insert({'test': 'object'})
        self.assertEqual(foobar_collection.find_one({}, {'_id': 0}),
                         {'test': 'object'})
        foobar_collection.remove()



    def test_MongoConfig_env(self):
        original_val = os.environ.get('foobar')
        os.environ['foobar'] = json.dumps({'database': 'local',
                                           'host': 'localhost',
                                           'port': 27017})

        config = md.MongoConfig(environ_var='foobar')

        mongo_db = config.db()
        foobar_collection = mongo_db['foobar']
        foobar_collection.remove()
        foobar_collection.insert({'test': 'object'})
        self.assertEqual(foobar_collection.find_one({}, {'_id': 0}),
                         {'test': 'object'})
        foobar_collection.remove()
        if original_val is None:
            del os.environ['foobar']
        else:
            os.environ['foobar'] = original_val


    '''
    ##########################################################################
    #                                                                        #
    #                       TEST HELPER FUNCTIONS                            #
    #                                                                        #
    ##########################################################################
    '''

    def test_convert_arg_soup(self):
        """ Tests we can convert args/kwargs to just kwargs """

        def test_func(arg1, arg2, kwarg1=None, kwarg2=None):
            pass

        callargs_1 = md.convert_arg_soup(test_func, 1, 2, 3, 4)
        self.assertEqual(callargs_1, {'arg1': 1, 'arg2': 2,
                                      'kwarg1': 3, 'kwarg2': 4})
        callargs_2 = md.convert_arg_soup(test_func, 1, 2)
        self.assertEqual(callargs_2, {'arg1': 1, 'arg2': 2,
                                      'kwarg1': None, 'kwarg2': None})

        callargs_3 = md.convert_arg_soup(test_func, 1, 2, kwarg2=4, kwarg1=3)
        self.assertEqual(callargs_3, {'arg1': 1, 'arg2': 2,
                                      'kwarg1': 3, 'kwarg2': 4})


    def test_replace_arg(self):
        """ Tests we can replace args by name """

        def dummy_replacer(argname, callargs=None, cdict=None):
            callargs = callargs or {}
            callargs[argname] = (cdict or {}).get(argname, 420)
            return callargs

        def f(arg1, arg2):
            return arg1 + arg2

        self.assertEqual(f(1, 2), 3)

        wrap_1 = md.replace_arg('arg1', dummy_replacer)
        self.assertEqual(wrap_1(f, {'arg1': 2, 'arg2': 10}), 430)


        wrap_2 = md.replace_arg('arg1', dummy_replacer, cdict={'arg1': 990})
        self.assertEqual(wrap_2(f, {'arg1': 2, 'arg2': 10}), 1000)



    def test_mongo_timeout_wrap(self):

        def f(arg1):
            return True


        def g(arg1, counter=[0]):
            if counter[0] == 0:
                counter[0] += 1
                print "NetworkTimeout"
                raise NetworkTimeout("FOO")
            elif counter[0] == 1:
                counter[0] += 1
                print "Connection Failure"
                raise ConnectionFailure("BAR")
            else:
                return True


        self.assertTrue(md.mongo_timeout_wrap(f, None, {'arg1': None}))
        self.assertTrue(md.mongo_timeout_wrap(g, None, {'arg1': None}))


    def test_modify_agg_pipeline(self):
        with self.assertRaises(AssertionError):
            md.modify_agg_pipeline('not pipeline', None, {})

        new_callargs = md.modify_agg_pipeline('pipeline',
                          {'update_filter': {'a': 'b', 'c': 'd'}},
                          {'pipeline': [{'$match': {'foo': 'bar'}}],
                           'alpha': 'beta'})

        self.assertEqual(new_callargs,
                         {'alpha': 'beta',
                          'pipeline': [{'$match': {'a': 'b', 'c': 'd'}},
                                       {'$match': {'foo': 'bar'}}]})


    def test_update_filter(self):

        # Test that we can update a None filter
        callargs_1 = {'filter': None, 'foo': 'bar'}
        md.update_filter('filter', {'update_filter': {'a': 'b', 'c': 'd'}},
                         callargs_1)
        self.assertEqual(callargs_1, {'filter': {'a': 'b', 'c': 'd'},
                                      'foo': 'bar'})

        # Tests that we can update a dict filter
        callargs_2 = {'filter': {'_id': 'ID'}, 'foo': 'bar'}
        md.update_filter('filter', {'update_filter': {'a': 'b', 'c': 'd',
                                                      '_id': 'NOTID'}},
                         callargs_2)
        self.assertEqual(callargs_2, {'filter': {'a': 'b', 'c': 'd',
                                                 '_id': 'ID'}, 'foo': 'bar'})

        # Tests that we can update an _id filter
        callargs_3 = {'filter': 'ID', 'foo': 'bar'}
        md.update_filter('filter', {'update_filter': {'a': 'b', 'c': 'd',
                                                      '_id': 'NOTID'}},
                         callargs_3)
        self.assertEqual(callargs_3, {'filter': {'a': 'b', 'c': 'd',
                                                 '_id': 'ID'}, 'foo': 'bar'})



if __name__ == '__main__':
    unittest.main()


