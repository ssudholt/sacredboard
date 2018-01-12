import json
import os

from sacredboard.app.data.errors import NotFoundError
from sacredboard.app.data.postgres.pgcursor import PostgresCursor
from sacredboard.app.data.rundao import RunDAO

class PostgresRunDAO(RunDAO):
    '''
    classdocs
    '''


    def __init__(self, pg_storage):
        '''
        Constructor
        '''
        self.pg_storage = pg_storage
        self.json_columns = ['config', 'info']
    
    def get(self, run_id):
        '''
        Return the run associated with the id.

        :raise NotFoundError when not found
        '''
        with self.pg_storage.get_db_connection() as dbcon:
            with dbcon.cursor() as cur:
                qry = 'SELECT * FROM run where id=%(run_id)s'
                cur.execute(qry, dict(run_id=run_id))
                colnames = [desc[0] for desc in cur.description]
                result = cur.fetchall()
                if len(result) == 0:
                    raise NotFoundError('The desired ID is not in the Database')
                elif len(result) != 1:
                    raise ValueError('Found multiple values for the desired ID')
                else:
                    result = result[0]
        output_dict = dict()
        for colname, value in zip(colnames, result):
            # if a column contains a JSON string convert it to a dict
            if colname in self.json_columns:
                value = json.loads(value)
            # rename id column to _id
            if colname == 'id':
                colname = '_id'
            output_dict[colname] = value
        output_dict['experiment'] = dict(name='EvalAdam', md5sum='blabla', base_dir='~/base')
        output_dict['host'] = dict(cpu='i7', os='Debian 8.0', python_version='2.7.9', hostname='rosenblatt')
        return output_dict

    def get_runs(self, sort_by=None, sort_direction=None,
                 start=0, limit=None, query={"type": "and", "filters": []}):
        '''
        Return all runs in the postgres database.

        :param start: NotImplemented
        :param limit: NotImplemented
        :param query: NotImplemented
        :return: PostgresCursor
        '''
        with self.pg_storage.get_db_connection() as dbcon:
            with dbcon.cursor() as cur:
                qry = 'SELECT id FROM run'
                if sort_by is not None:
                    qry += ' ORDER BY %s' % sort_by
                    if sort_direction is not None:
                        qry += ' %s' % sort_direction.upper()
                cur.execute(qry)
                result = cur.fetchall()
        # get all experiment IDs which are not None
        run_ids = [elem[0] for elem in result if elem[0] is not None]
        
        def run_iterator():
            for run_id in run_ids:
                yield self.get(run_id)

        count = len(run_ids)
        return PostgresCursor(count, run_iterator())

    def delete(self, run_id):
        '''
        Delete run with the given id from the backend.

        :param run_id: Id of the run to delete.
        :raise DataSourceError General data source error.
        :raise NotFoundError The run was not found. (Some backends may succeed
        even if the run does not exist.
        '''
        with self.pg_storage.get_db_connection() as dbcon:
            with dbcon.cursor() as cur:
                # get all artifacts and delete them
                qry = 'SELECT filename FROM artifacts WHERE run_id=%s'
                cur.execute(qry, (run_id,))
                for elem in cur.fetchall():
                    if os.path.exists(elem[0]):
                        os.remove(elem[0])
                # delete row in run table
                qry = 'DELETE FROM run WHERE id=%s'
                cur.execute(qry, (run_id,))
        