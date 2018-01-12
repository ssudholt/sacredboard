from sacredboard.app.data.datastorage import Cursor

class PostgresCursor(Cursor):
    '''Implements Cursor for postgres.'''

    def __init__(self, count, iterable):
        ''''Initialize PostgresCursor with a given iterable.'''
        self.iterable = iterable
        self._count = count

    def count(self):
        '''
        Return the number of runs in this query.

        :return: int
        '''
        return self._count

    def __iter__(self):
        ''''Iterate over runs.'''
        return iter(self.iterable)