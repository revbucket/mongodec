""" Tests for filter_mongo.py """

import unittest
import mongodec.mongodec as md
import mongodec.filter_mongo as fm


######################################################################
#   Helper methods useful in tests                                   #
######################################################################

def get_local_mongo():
    return md.MongoConfig(user=None, password=None, database='local',
                          host='localhost', port=27017).db()

def drop_collections(mongo_db):
    for coll in mongo_db.collection_names():
        try:
            mongo_db.drop_collection(coll)
        except:
            pass


'''
##############################################################################
#                                                                            #
#                           ACTUAL TEST CASE                                 #
#                                                                            #
##############################################################################
'''

class TestFilterMongo(unittest.TestCase):

    def setUp(self):
        drop_collections(get_local_mongo())

    def tearDown(self):
        drop_collections(get_local_mongo())


    def test_FilterMongoDB(self):
        config = md.MongoConfig(user=None, password=None, database='local',
                                host='localhost', port=27017)
        mongo_db = config.db()
        filt = {'id': {'$gt': 420}}
        filter_mongo = fm.FilterMongoDB(mongo_db, _filter=filt)

        # Check this works like a regular database
        self.assertEqual(filter_mongo.base_object, mongo_db)
        self.assertEqual(filter_mongo.name, 'local')

        # Check we can access each collection 4 ways
        coll1 = filter_mongo.collection_1
        coll2 = filter_mongo['collection_2']
        coll3 = filter_mongo.create_collection('collection_3')
        coll4 = filter_mongo.get_collection('collection_4')

        for i, coll in enumerate([coll1, coll2, coll3, coll4], start=1):
            self.assertTrue(isinstance(coll, fm.FilterMongoCollection))
            self.assertEqual(coll.name, 'collection_%s' % i)
            self.assertEqual(coll._filter, filt)


    def test_FilterMongoCollection_base(self):
        """ Just tests that we can build a filter collection object w/o err """
        config = md.MongoConfig(user=None, password=None, database='local',
                                host='localhost', port=27017)
        coll = config.db()['stamp_collection']
        filter_coll = fm.FilterMongoCollection(coll, _filter={'foo': 'bar'})

        self.assertEqual(filter_coll.name, coll.name)
        self.assertEqual(filter_coll.codec_options, coll.codec_options)


    ##########################################################################
    #   Methods to test each of the filter operations                        #
    ##########################################################################


    def test_count(self):
        """ ChangelingMongoCollection.count """
        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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


    def test_replace_one(self):
        """ ChangelingMongoCollection.replace_one """
        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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




    def test_update_one(self):
        """ ChangelingMongoCollection.update_one """
        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_update_many(self):
        """ ChangelingMongoCollection.update_many """
        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_delete_one(self):
        """ ChangelingMongoCollection.delete_one """
        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_delete_many(self):
        """ ChangelingMongoCollection.delete_many """

        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_find_one_and_delete(self):
        """ ChangelingMongoCollection.find_one_and_delete """

        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_find_one_and_replace(self):
        """ ChangelingMongoCollection.find_one_and_replace """

        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_find_one_and_update(self):
        """ ChangelingMongoCollection.find_one_and_update """

        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_distinct(self):
        """ ChangelingMongoCollection.distinct """

        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_update(self):
        """ ChangelingMongoCollection.update """

        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_remove(self):
        """ ChangelingMongoCollection.remove """

        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_aggregate(self):
        """ChangelingMongoCollection.find"""

        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_find(self):
        """ChangelingMongoCollection.find"""

        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_find_one(self):
        """ChangelingMongoCollection.find_one"""

        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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



    def test_bulkop(self):
        """FilterMongoBulkOperationBuilder """
        r_mongo_db = get_local_mongo()
        c_mongo_db = fm.FilterMongoDB(r_mongo_db, _filter={'name': 'foobar'})
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




if __name__ == '__main__':
    unittest.main()
