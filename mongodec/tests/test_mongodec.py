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
    #                       TEST FILTER MONGO STUFF                          #
    #                                                                        #
    ##########################################################################
    '''
    @clear_db_decorator
    def test_FilterMongoDB(self):
        config = md.MongoConfig(user=None, password=None, database='local',
                                host='localhost', port=27017)
        mongo_db = config.db()
        filt = {'id': {'$gt': 420}}
        filter_mongo = md.FilterMongoDB(mongo_db, _filter=filt)

        # Check this works like a regular database
        self.assertEqual(filter_mongo.base_object, mongo_db)
        self.assertEqual(filter_mongo.name, 'local')

        # Check we can access each collection 4 ways
        coll1 = filter_mongo.collection_1
        coll2 = filter_mongo['collection_2']
        coll3 = filter_mongo.create_collection('collection_3')
        coll4 = filter_mongo.get_collection('collection_4')

        for i, coll in enumerate([coll1, coll2, coll3, coll4], start=1):
            self.assertTrue(isinstance(coll, md.FilterCollection))
            self.assertEqual(coll.name, 'collection_%s' % i)
            self.assertEqual(coll._filter, filt)


    def test_FilterCollection_base(self):
        """ Just tests that we can build a filter collection object w/o err """
        config = md.MongoConfig(user=None, password=None, database='local',
                                host='localhost', port=27017)
        coll = config.db()['stamp_collection']
        filter_coll = md.FilterCollection(coll, _filter={'foo': 'bar'})

        self.assertEqual(filter_coll.name, coll.name)
        self.assertEqual(filter_coll.codec_options, coll.codec_options)


    ##########################################################################
    #   Methods to test each of the filter operations                        #
    ##########################################################################

    @clear_db_decorator
    def test_count(self):
        """ ChangelingMongoCollection.count """
        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']

        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})




        self.assertEqual(r_coll.count(), 3)
        self.assertEqual(c_coll.count(), 2)

        self.assertEqual(r_coll.count({'id': 'a'}), 2)
        self.assertEqual(c_coll.count(filter={'id': 'a'}), 1)

        self.assertEqual(r_coll.count(), 3)
        self.assertEqual(c_coll.count(no_changeling=True), 3)

        self.assertEqual(r_coll.count({'id': 'a'}), 2)
        self.assertEqual(c_coll.count({'id': 'a'}, no_changeling=True), 2)

        self.assertEqual(c_coll.count({'name': 'foobaz'}), 1)

    @clear_db_decorator
    def test_replace_one(self):
        """ ChangelingMongoCollection.replace_one """
        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']


        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})
        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        self.assertIsNotNone(r_coll.find_one({'val': 420}))

        c_coll.replace_one({'val': 420}, {'name': 'foobar', 'id': 'a',
                                         'val': 421})
        c_coll.replace_one({'val': 840}, {'name': 'foobaz', 'id': 'a',
                                         'val': 841})
        self.assertIsNotNone(r_coll.find_one({'val': 840}))

        self.assertIsNone(r_coll.find_one({'val': 420}))

        c_coll.replace_one({'val': 840}, {'name': 'foobaz', 'id': 'a',
                                         'val': 841},
                            no_changeling=True)
        self.assertIsNone(r_coll.find_one({'val': 840}))



    @clear_db_decorator
    def test_update_one(self):
        """ ChangelingMongoCollection.update_one """
        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']


        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})

        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        c_coll.update_one({'val': 840}, {'$set': {'val': 841}})
        self.assertIsNotNone(r_coll.find_one({'val': 840}))

        self.assertIsNotNone(r_coll.find_one({'val': 420}))
        c_coll.update_one({'val': 420}, {'$set': {'val': 421}})
        self.assertIsNone(r_coll.find_one({'val': 420}))

        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        c_coll.update_one({'val': 840}, {'$set': {'val': 841}},
                          no_changeling=True)
        self.assertIsNone(r_coll.find_one({'val': 840}))


    @clear_db_decorator
    def test_update_many(self):
        """ ChangelingMongoCollection.update_many """
        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']

        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})

        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        c_coll.update_many({'val': 840}, {'$set': {'val': 841}})
        self.assertIsNotNone(r_coll.find_one({'val': 840}))

        self.assertEqual(r_coll.count({'id': 'a'}), 3)
        c_coll.update_many({'id': 'a'}, {'$set': {'id': 'c'}})
        self.assertEqual(r_coll.count({'id': 'a'}), 1)

        r_coll.update_many({'id': 'c'}, {'$set': {'id': 'a'}})


        self.assertEqual(r_coll.count({'id': 'a'}), 3)
        c_coll.update_many({'id': 'a'}, {'$set': {'id': 'c'}},
                           no_changeling=True)
        self.assertEqual(r_coll.count({'id': 'a'}), 0)


    @clear_db_decorator
    def test_delete_one(self):
        """ ChangelingMongoCollection.delete_one """
        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']

        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})

        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        c_coll.delete_one({'val': 840})
        self.assertIsNotNone(r_coll.find_one({'val': 840}))


        self.assertIsNotNone(r_coll.find_one({'val': 420}))
        c_coll.delete_one({'val': 420})
        self.assertIsNone(r_coll.find_one({'val': 420}))

        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        c_coll.delete_one({'val': 840}, no_changeling=True)
        self.assertIsNone(r_coll.find_one({'val': 840}))


    @clear_db_decorator
    def test_delete_many(self):
        """ ChangelingMongoCollection.delete_many """

        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']


        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})


        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        c_coll.delete_many({'val': 840})
        self.assertIsNotNone(r_coll.find_one({'val': 840}))


        self.assertEqual(r_coll.count({'id': 'a'}), 3)
        c_coll.delete_many({'id': 'a'})
        self.assertEqual(r_coll.count({'id': 'a'}), 1)

        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        self.assertEqual(r_coll.count({'id': 'a'}), 3)
        c_coll.delete_many({'id': 'a'}, no_changeling=True)
        self.assertEqual(r_coll.count({'id': 'a'}), 0)


    @clear_db_decorator
    def test_find_one_and_delete(self):
        """ ChangelingMongoCollection.find_one_and_delete """

        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']


        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})

        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        self.assertIsNone(c_coll.find_one_and_delete({'val': 840}, {'_id': 0}))
        self.assertIsNotNone(r_coll.find_one({'val': 840}))


        self.assertIsNotNone(r_coll.find_one({'val': 420}))
        out_1 = c_coll.find_one_and_delete({'val': 420}, {'_id': 0})
        self.assertEqual(out_1,
                         {'name': 'foobar', 'id': 'a', 'val': 420})
        self.assertIsNone(r_coll.find_one({'val': 420}))


        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        out_2 = c_coll.find_one_and_delete({'val': 840}, {'_id': 0},
                                            no_changeling=True)
        self.assertEqual(out_2,
                         {'name': 'foobaz', 'id': 'a', 'val': 840})
        self.assertIsNone(r_coll.find_one({'val': 840}))


    @clear_db_decorator
    def test_find_one_and_replace(self):
        """ ChangelingMongoCollection.find_one_and_replace """

        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']


        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})

        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        self.assertIsNotNone(r_coll.find_one({'val': 420}))

        out1 = c_coll.find_one_and_replace({'val': 420},
                                           {'name': 'foobar',
                                            'id': 'a', 'val': 421},
                                           {'_id': 0})
        self.assertEqual(out1, {'val': 420, 'name': 'foobar', 'id': 'a'})

        out2 = c_coll.find_one_and_replace({'val': 840},
                                           {'name': 'foobaz', 'id': 'a',
                                            'val': 841}, {'_id': 0})
        self.assertIsNone(out2)
        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        self.assertIsNone(r_coll.find_one({'val': 420}))

        out3 = c_coll.find_one_and_replace({'val': 840},
                                           {'name': 'foobaz', 'id': 'a',
                                           'val': 841}, {'_id': 0},
                                           no_changeling=True)
        self.assertEqual(out3, {'name': 'foobaz', 'id': 'a', 'val': 840})
        self.assertIsNone(r_coll.find_one({'val': 840}))


    @clear_db_decorator
    def test_find_one_and_update(self):
        """ ChangelingMongoCollection.find_one_and_update """

        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']


        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})
        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        self.assertIsNotNone(r_coll.find_one({'val': 420}))

        out1 = c_coll.find_one_and_update({'val': 420}, {'$set': {'val': 421}},
                                          {'_id': 0})

        self.assertEqual(out1, {'val': 420, 'name': 'foobar', 'id': 'a'})

        out2 = c_coll.find_one_and_update({'val': 840}, {'$set': {'val': 841}})
        self.assertIsNone(out2)
        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        self.assertIsNone(r_coll.find_one({'val': 420}))

        out3 = c_coll.find_one_and_update({'val': 840},
                                          {'$set': {'val': 841}}, {'_id': 0},
                                          no_changeling=True)
        self.assertEqual(out3, {'name': 'foobaz', 'id': 'a', 'val': 840})
        self.assertIsNone(r_coll.find_one({'val': 840}))


    @clear_db_decorator
    def test_distinct(self):
        """ ChangelingMongoCollection.distinct """

        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']



        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 0})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 1})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 0})
        r_coll.insert({'name': 'foobaz', 'id': 'c', 'val': 1})

        self.assertEqual(sorted(c_coll.distinct('id')), ['a', 'b'])
        self.assertEqual(sorted(r_coll.distinct('id')), ['a', 'b', 'c'])

        self.assertEqual(sorted(c_coll.distinct('id', {'val': 1})), ['b'])
        self.assertEqual(sorted(r_coll.distinct('id', {'val': 1})), ['b', 'c'])


        self.assertEqual(sorted(c_coll.distinct('id', no_changeling=True)),
                         sorted(r_coll.distinct('id')))

        self.assertEqual(sorted(c_coll.distinct('id', {'val': 1},
                                                 no_changeling=True)),
                         sorted(r_coll.distinct('id', {'val': 1})))

        self.assertEqual(sorted(c_coll.distinct('id',
                                                {'name': 'foobaz'})),
                         ['a', 'c'])


    @clear_db_decorator
    def test_update(self):
        """ ChangelingMongoCollection.update """

        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']


        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})

        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        c_coll.update({'val': 840}, {'$set': {'val': 841}})
        self.assertIsNotNone(r_coll.find_one({'val': 840}))

        self.assertIsNotNone(r_coll.find_one({'val': 420}))
        c_coll.update({'val': 420}, {'$set': {'val': 421}})
        self.assertIsNone(r_coll.find_one({'val': 420}))

        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        c_coll.update({'val': 840}, {'$set': {'val': 841}},
                       no_changeling=True)
        self.assertIsNone(r_coll.find_one({'val': 840}))

        r_mongo_db.drop_collection('dummyColl')
        self.assertEqual(r_coll.count(), 0)

        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})

        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        c_coll.update({'val': 840}, {'$set': {'val': 841}}, multi=True)
        self.assertIsNotNone(r_coll.find_one({'val': 840}))

        self.assertEqual(r_coll.count({'id': 'a'}), 3)
        c_coll.update({'id': 'a'}, {'$set': {'id': 'c'}}, multi=True)
        self.assertEqual(r_coll.count({'id': 'a'}), 1)

        r_coll.update({'id': 'c'}, {'$set': {'id': 'a'}}, multi=True)

        self.assertEqual(r_coll.count({'id': 'a'}), 3)
        c_coll.update({'id': 'a'}, {'$set': {'id': 'c'}},
                      no_changeling=True, multi=True)
        self.assertEqual(r_coll.count({'id': 'a'}), 0)


    @clear_db_decorator
    def test_remove(self):
        """ ChangelingMongoCollection.remove """

        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']


        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})

        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        c_coll.remove({'val': 840})
        self.assertIsNotNone(r_coll.find_one({'val': 840}))


        self.assertIsNotNone(r_coll.find_one({'val': 420}))
        c_coll.remove({'val': 420})
        self.assertIsNone(r_coll.find_one({'val': 420}))

        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        c_coll.remove({'val': 840}, no_changeling=True)
        self.assertIsNone(r_coll.find_one({'val': 840}))



        r_mongo_db.drop_collection('dummyColl')
        self.assertEqual(r_coll.count(), 0)

        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 123})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 840})


        self.assertIsNotNone(r_coll.find_one({'val': 840}))
        c_coll.remove({'val': 840}, multi=True)
        self.assertIsNotNone(r_coll.find_one({'val': 840}))


        self.assertEqual(r_coll.count({'id': 'a'}), 3)
        c_coll.remove({'id': 'a'}, multi=True)
        self.assertEqual(r_coll.count({'id': 'a'}), 1)

        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 420})
        self.assertEqual(r_coll.count({'id': 'a'}), 3)
        c_coll.remove({'id': 'a'}, no_changeling=True, multi=True)
        self.assertEqual(r_coll.count({'id': 'a'}), 0)


    @clear_db_decorator
    def test_aggregate(self):
        """ChangelingMongoCollection.find"""

        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']


        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 0})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 1})
        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 2})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 3})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 4})


        pipeline = [{'$group': {'_id': '$id', 'count': {'$sum': 1}}}]

        self.assertEqual(sorted(r_coll.aggregate(pipeline),
                                key=lambda x: x['_id']),
                         [{'_id': 'a', 'count': 3}, {'_id': 'b', 'count': 2}])

        self.assertEqual(sorted(c_coll.aggregate(pipeline),
                                key=lambda x: x['_id']),
                         [{'_id': 'a', 'count': 2}, {'_id': 'b', 'count': 2}])

        self.assertEqual(sorted(c_coll.aggregate(pipeline, no_changeling=True),
                                key=lambda x: x['_id']),
                         [{'_id': 'a', 'count': 3}, {'_id': 'b', 'count': 2}])


    @clear_db_decorator
    def test_find(self):
        """ChangelingMongoCollection.find"""

        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']


        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 0})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 1})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 2})
        r_coll.insert({'name': 'foobaz', 'id': 'c', 'val': 3})

        self.assertEqual(sorted(list(c_coll.find())),
                         sorted([_ for _ in r_coll.find()
                                 if _['name'] == 'foobar']))
        self.assertEqual(list(c_coll.find({'id': 'a'}, {'_id': 0})),
                         [{'name': 'foobar', 'id': 'a', 'val': 0}])

        self.assertEqual(sorted(list(r_coll.find({'id': 'a'}, {'_id': 0})),
                                key=lambda d: d['val']),
                             [{'name': 'foobar', 'id': 'a', 'val': 0},
                              {'name': 'foobaz', 'id': 'a', 'val': 2}])

        self.assertEqual(list(c_coll.find({'id': 'a',
                                           'name': 'foobaz'},
                                          {'_id': 0})),
                         [{'name': 'foobaz', 'id': 'a', 'val': 2}])

        self.assertEqual(sorted(list(c_coll.find(no_changeling=True))),
                         sorted(list(r_coll.find())))


    @clear_db_decorator
    def test_find_one(self):
        """ChangelingMongoCollection.find_one"""

        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']


        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 0})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 1})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 2})
        r_coll.insert({'name': 'foobaz', 'id': 'c', 'val': 3})

        self.assertEqual(c_coll.find_one({'id': 'a'}, {'_id': 0}),
                         {'name': 'foobar', 'id': 'a', 'val': 0})

        self.assertEqual(c_coll.find_one({'val': 3}, {'_id': 0}), None)

        self.assertEqual(c_coll.find_one({'val': 3}, {'_id': 0},
                                         no_changeling=True),
                         {'name': 'foobaz', 'id': 'c', 'val': 3})
        self.assertEqual(c_coll.find_one({'val': 3, 'name': 'foobaz'},
                                         {'_id': 0},  no_changeling=True),
                         {'name': 'foobaz', 'id': 'c', 'val': 3})


    @clear_db_decorator
    def test_bulkop(self):
        """FilterMongoBulkOperationBuilder """
        r_mongo_db = get_local_mongo()
        c_mongo_db = md.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
        r_coll = r_mongo_db['dummyColl']
        c_coll = c_mongo_db['dummyColl']


        r_coll.insert({'name': 'foobar', 'id': 'a', 'val': 0})
        r_coll.insert({'name': 'foobar', 'id': 'b', 'val': 1})
        r_coll.insert({'name': 'foobaz', 'id': 'a', 'val': 2})
        r_coll.insert({'name': 'foobaz', 'id': 'c', 'val': 3})


        c_bulkop = c_coll.initialize_unordered_bulk_op()

        for i in xrange(4):
            c_bulkop.find({'val': i}).update({'$set': {'val': 10 * i - 3}})
        c_bulkop.execute()


        exp_out = [{'name': 'foobar', 'id': 'a', 'val': -3},
                   {'name': 'foobar', 'id': 'b', 'val': 7},
                   {'name': 'foobaz', 'id': 'a', 'val': 2},
                   {'name': 'foobaz', 'id': 'c', 'val': 3},]
        self.assertEqual(sorted(r_coll.find({}, {'_id': 0}),
                                            key=lambda d: d['val']),
                         sorted(exp_out, key=lambda d: d['val']))


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


