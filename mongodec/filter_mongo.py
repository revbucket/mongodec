from mongodec import mongo_timeout_wrap, modify_agg_pipeline, update_filter
from changeling import Changeling, replace_arg
from pymongo.collection import Collection


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



class FilterMongoCollection(Changeling):

    def __init__(self, base_object, _filter=None, timeout_wrap=True):
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
        if timeout_wrap:
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

