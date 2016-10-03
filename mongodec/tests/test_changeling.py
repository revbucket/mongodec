import unittest
#import Changeling
from mongodec.changeling import Changeling, replace_arg, convert_arg_soup


class TestChangeling(unittest.TestCase):
    def test_Changeling(self):
        """ Simple changeling test """
        # Test that we can make a simple dummy class
        return
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

        c_foo_instance = Changeling(foo_instance,
                                    cdict={'Foo_methods': {'f': dummy_wrap}})

        self.assertEqual(c_foo_instance.f(12), 432)
        self.assertEqual(c_foo_instance.f(12, no_changeling=True), 32)
        self.assertEqual(c_foo_instance.prop, 'harambe died for our sins')

        c_wrap_all = Changeling(foo_instance,
                                cdict={'Foo_methods': {'f': dummy_wrap},
                                       'Foo_wrap_all': dummy_wrap})

        self.assertEqual(c_wrap_all.f(12), 832)
        self.assertEqual(c_wrap_all.f(12, no_changeling=True), 32)


    def test_convert_arg_soup(self):
        """ Tests we can convert args/kwargs to just kwargs """

        def test_func(arg1, arg2, kwarg1=None, kwarg2=None):
            pass

        callargs_1 = convert_arg_soup(test_func, 1, 2, 3, 4)
        self.assertEqual(callargs_1, {'arg1': 1, 'arg2': 2,
                                      'kwarg1': 3, 'kwarg2': 4})
        callargs_2 = convert_arg_soup(test_func, 1, 2)
        self.assertEqual(callargs_2, {'arg1': 1, 'arg2': 2,
                                      'kwarg1': None, 'kwarg2': None})

        callargs_3 = convert_arg_soup(test_func, 1, 2, kwarg2=4, kwarg1=3)
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

        wrap_1 = replace_arg('arg1', dummy_replacer)
        self.assertEqual(wrap_1(f, {'arg1': 2, 'arg2': 10}), 430)


        wrap_2 = replace_arg('arg1', dummy_replacer, cdict={'arg1': 990})
        self.assertEqual(wrap_2(f, {'arg1': 2, 'arg2': 10}), 1000)

if __name__ == '__main__':
    unittest.main()