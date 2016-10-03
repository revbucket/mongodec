
import inspect

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
##############################################################################
#                                                                            #
#                           CHANGELING HELPERS                               #
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
