# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 Matt Jordan, mattjordan.mail@gmail.com
#
# This module is part of mongodec and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause

# Setup namespace

from mongodec import MongoConfig
from changeling import Changeling
from filter_mongo import FilterMongoDB, \
                         FilterMongoCollection, \
                         FilterMongoBulkOperationBuilder

__version__ = '1.0.5'
__all__ = [filter_mongo, mongodec]



