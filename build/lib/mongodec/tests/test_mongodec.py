""" Tests for mongodec.py """

import unittest
import mongodec.mongodec as md
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


