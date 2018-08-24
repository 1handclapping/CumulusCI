from cumulusci.tasks.salesforce import BaseSalesforceApiTask

from contextlib import contextmanager
import csv
import datetime
import gzip
import io
import itertools
import os
import requests
import shutil
import tempfile
import time
import unicodecsv
import urlparse
import xml.etree.ElementTree as ET

import hiyapyco
import unicodecsv

from collections import OrderedDict

from salesforce_bulk import CsvDictsAdapter
from salesforce_bulk.util import IteratorBytesIO

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import aliased
from sqlalchemy.orm import create_session
from sqlalchemy.orm import mapper
from sqlalchemy.orm import Session
from sqlalchemy import bindparam
from sqlalchemy import create_engine
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Unicode
from sqlalchemy import text
from sqlalchemy import types
from sqlalchemy import event

# TODO: UserID Catcher
# TODO: Dater

# Create a custom sqlalchemy field type for sqlite datetime fields which are stored as integer of epoch time
class EpochType(types.TypeDecorator):
    impl = types.Integer

    epoch = datetime.datetime(1970, 1, 1, 0, 0, 0)

    def process_bind_param(self, value, dialect):
        return int((value - self.epoch).total_seconds()) * 1000

    def process_result_value(self, value, dialect):
        return self.epoch + datetime.timedelta(seconds=value / 1000)

# Listen for sqlalchemy column_reflect event and map datetime fields to EpochType
@event.listens_for(Table, "column_reflect")
def setup_epoch(inspector, table, column_info):
    if isinstance(column_info['type'], types.DateTime):
        column_info['type'] = EpochType()

class DeleteData(BaseSalesforceApiTask):

    task_options = {
        'objects': {
            'description': 'A list of objects to delete records from in order of deletion.  If passed via command line, use a comma separated string',
            'required': True,
        },
        'operation': {
            'description': 'Bulk operation to perform (delete or hardDelete)',
            'required': True,
        }
    }

    def _init_options(self, kwargs):
        super(DeleteData, self)._init_options(kwargs)
       
        # Split and trim objects string into a list if not already a list
        if not isinstance(self.options['objects'], list):
            self.options['objects'] = [obj.strip() for obj in self.options['objects'].split(',')]

    def _run_task(self):
        for obj in self.options['objects']:
            self.logger.info('Deleting all {} records'.format(obj))
            # Query for all record ids
            self.logger.info('  Querying for all {} objects'.format(obj))
            query_job = self.bulk.create_query_job(obj, contentType='CSV')
            batch = self.bulk.query(query_job, "select Id from {}".format(obj))
            while not self.bulk.is_batch_done(batch, query_job):
                time.sleep(10)
            self.bulk.close_job(query_job)
            delete_rows = []
            for result in self.bulk.get_all_results_for_query_batch(batch,query_job):
                reader = unicodecsv.DictReader(result, encoding='utf-8')
                for row in reader:
                    delete_rows.append(row)

            if not delete_rows:
                self.logger.info('  No {} objects found, skipping delete'.format(obj))
                continue

            # Delete the records
            delete_job = self.bulk.create_job(obj, self.options['operation'])
            self.logger.info('  Deleting {} {} records'.format(len(delete_rows), obj))
            batch_num = 1
            for batch in self._upload_batch(delete_job, delete_rows):
                self.logger.info('    Uploaded batch {}'.format(batch))
                batch_num += 1
            self.bulk.close_job(delete_job)

            while True:
                job_status = self.bulk.job_status(delete_job)
                self.logger.info('    Waiting for job {} ({}/{})'.format(
                    delete_job,
                    job_status['numberBatchesCompleted'],
                    job_status['numberBatchesTotal'],
                ))
                batch_status = self._get_batch_status(delete_job)
                if batch_status == 'InProgress':
                    time.sleep(10)
                    continue
                self.logger.info('  Job {} finished with result: {}'.format(delete_job, batch_status))
                break

    def _get_batch_status(self, job_id):
        uri = urlparse.urljoin(self.bulk.endpoint + "/", 'job/{0}/batch'.format(job_id))
        response = requests.get(uri, headers=self.bulk.headers())
        completed = True
        tree = ET.fromstring(response.content)
        for el in tree.iterfind('.//{%s}state' % self.bulk.jobNS):
            state = el.text
            if state == 'Failed':
                return 'Failed'
            if state != 'Completed':
                completed = False
        return 'Completed' if completed else 'InProgress'

    def _split_batches(self, data, batch_size):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(data), batch_size):
            yield data[i:i + batch_size]

    def _upload_batch(self, job, data):
        # Split into batches
        batches = self._split_batches(data, 10000)

        uri = "{}/job/{}/batch".format(self.bulk.endpoint, job)
        headers = self.bulk.headers({"Content-Type": "text/csv"})
        for batch in batches:
            data = ['"Id"']
            data += ['"{}"'.format(record['Id']) for record in batch]
            data = '\n'.join(data)
            resp = requests.post(uri, data=data, headers=headers)
            content = resp.content

            if resp.status_code >= 400:
                self.bulk.raise_error(content, resp.status_code)

            tree = ET.fromstring(content)
            batch_id = tree.findtext("{%s}id" % self.bulk.jobNS)

            yield batch_id

class LoadData(BaseSalesforceApiTask):

    task_options = {
        'database_url': {
            'description': 'The database url to a database containing the test data to load',
            'required': True,
        },
        'mapping': {
            'description': 'The path to a yaml file containing mappings of the database fields to Salesforce object fields',
            'required': True,
        },
        'step': {
            'description': 'If specified, only run this step from the mapping',
            'required': False,
        },
    }

    def _run_task(self):
        self._init_mapping()
        self._init_db()

        step = self.options.get('step')
        for name, mapping in self.mapping.items():
            if step and name != step:
                continue
            if mapping.get('retrieve_only', False):
                continue

            self.logger.info('Running Job: {}'.format(name))
            state = self._load_mapping(mapping)
            if state == 'Failed':
                self.logger.error('Job failed: {}'.format(name))
                break

    def _load_mapping(self, mapping):
        job_id = self.bulk.create_insert_job(mapping['sf_object'], contentType='CSV')
        self.logger.info('  Created bulk job {}'.format(job_id))

        # Upload batches
        local_ids_for_batch = {}
        for batch_file, local_ids in self._get_batches(mapping):
            batch_id = self.bulk.post_batch(job_id, batch_file)
            local_ids_for_batch[batch_id] = local_ids
            self.logger.info('    Uploaded batch {}'.format(batch_id))
        self.bulk.close_job(job_id)

        # Wait for job to complete
        while True:
            job_status = self.bulk.job_status(job_id)
            self.logger.info('    Waiting for job {} ({}/{})'.format(
                job_id,
                job_status['numberBatchesCompleted'],
                job_status['numberBatchesTotal'],
            ))
            batch_status = self._get_batch_status(job_id)
            if batch_status == 'InProgress':
                time.sleep(10)
                continue
            self.logger.info('  Job {} finished with result: {}'.format(job_id, batch_status))
            break

        if batch_status != 'Completed':
            return batch_status

        # Get results and create table with inserted ids
        if not hasattr(self, '_initialized_id_tables'):
            self._initialized_id_tables = set()
        id_table_name = '{}_sf_ids'.format(mapping['table'])
        if id_table_name not in self._initialized_id_tables:
            if id_table_name in self.metadata.tables:
                self.metadata.remove(self.metadata.tables[id_table_name])
            id_table = Table(
                id_table_name, self.metadata,
                Column('id', Unicode(255), primary_key=True),
                Column('sf_id', Unicode(18)),
            )
            if id_table.exists():
                id_table.drop()
            id_table.create()
            self._initialized_id_tables.add(id_table_name)
        conn = self.session.connection()
        for batch_id, local_ids in local_ids_for_batch.items():
            results_url = '{}/job/{}/batch/{}/result'.format(self.bulk.endpoint, job_id, batch_id)
            updates = []
            with _download_file(results_url, self.bulk) as f:
                self.logger.info('  Downloaded results for batch {}'.format(batch_id))

                def produce_csv():
                    reader = csv.reader(f)
                    next(reader)  # skip header
                    i = 0
                    for row, local_id in itertools.izip(reader, local_ids):
                        if row[1] == 'true':
                            sf_id = row[0]
                            yield '{},{}\n'.format(local_id, sf_id)
                        else:
                            self.logger.warning('      Error on row {}: {}'.format(i, row[3]))
                        i += 1

                with conn.connection.cursor() as cursor:
                    cursor.copy_expert(
                        'COPY {} ({}) FROM STDIN WITH (FORMAT CSV)'.format(
                            id_table_name,
                            b'id,sf_id',
                        ),
                        IteratorBytesIO(produce_csv()),
                    )
                    self.session.flush()

            self.logger.info('  Updated {} for batch {}'.format(id_table, batch_id))

        self.session.commit()
        return 'Completed'

    def _get_batch_status(self, job_id):
        uri = urlparse.urljoin(self.bulk.endpoint + "/", 'job/{0}/batch'.format(job_id))
        response = requests.get(uri, headers=self.bulk.headers())
        completed = True
        tree = ET.fromstring(response.content)
        for el in tree.iterfind('.//{%s}state' % self.bulk.jobNS):
            state = el.text
            if state == 'Failed':
                return 'Failed'
            if state != 'Completed':
                completed = False
        return 'Completed' if completed else 'InProgress'

    def _query_db(self, mapping):
        model = self.tables[mapping.get('table')]

        fields = mapping['fields'].copy()
        del fields['Id']
        fields = fields.values()
        lookups = mapping.get('lookups', {}).copy().values()
        lookup_columns = {}
        for lookup in lookups:
            lookup['aliased_table'] = aliased(
                self.metadata.tables['{}_sf_ids'.format(lookup['table'])]
            )
            lookup_columns[lookup['key_field']] = lookup['aliased_table'].columns.sf_id
        columns = [model.id]
        for c in model.__table__.columns:
            if c.key in fields:
                columns.append(c)
            elif c.key in lookup_columns:
                columns.append(lookup_columns[c.key])

        query = self.session.query(*columns)
        if 'record_type' in mapping:
            query = query.filter(model.record_type == mapping['record_type'])
        if 'filters' in mapping:
            filter_args = []
            for f in mapping['filters']:
                filter_args.append(text(f))
            query = query.filter(*filter_args)
        for lookup in lookups:
            # Outer join with lookup ids table:
            # returns main obj even if lookup is null
            value_column = getattr(model, lookup['key_field'])
            query = query.outerjoin(
                lookup['aliased_table'],
                lookup['aliased_table'].columns.id == value_column,
            )
            # Order by foreign key to minimize lock contention
            # by trying to keep lookup targets in the same batch
            lookup_column = getattr(model, lookup['key_field'])
            query = query.order_by(lookup_column)
        self.logger.info(str(query))
        return query

    def _get_batches(self, mapping, batch_size=10000):
        action = mapping.get('action', 'insert')
        fields = mapping.get('fields', {}).copy()
        static = mapping.get('static', {})
        lookups = mapping.get('lookups', {})
        record_type = mapping.get('record_type')

        # Skip Id field on insert
        id_column = fields.get('Id', 'id')
        if action == 'insert' and 'Id' in fields:
            del fields['Id']

        # Build the list of fields to import
        columns = []
        columns.extend(fields.keys())
        columns.extend(lookups.keys())
        columns.extend(static.keys())
        if record_type:
            columns.append('RecordTypeId')
            # default to the profile assigned recordtype if we can't find any
            # query for the RT by developer name
            try:
                query = "SELECT Id FROM RecordType WHERE SObjectType='{0}'" \
                    "AND DeveloperName = '{1}' LIMIT 1"
                record_type_id = self.sf.query(
                    query.format(mapping.get('sf_object'), record_type)
                )['records'][0]['Id']
            except (KeyError, IndexError):
                record_type_id = None

        query = self._query_db(mapping)

        total_rows = 0
        batch_num = 0
        batch_ids = []
        for row in query.yield_per(batch_size):
            # TODO iterate over raw encoded values
            if not total_rows % batch_size:
                if batch_num > 0:
                    batch_file.seek(0)
                    self.logger.info('    Processing batch {}'.format(batch_num))
                    yield batch_file, batch_ids
                batch_file = io.BytesIO()
                writer = csv.writer(batch_file)
                writer.writerow(columns)
                batch_ids = []
                batch_num += 1
            total_rows += 1

            # Add static values
            pkey = row[0]
            row = list(row[1:]) + static.values()
            if record_type:
                row.append(record_type_id)

            writer.writerow([self._convert(value) for value in row])
            batch_ids.append(pkey)

        if batch_ids:
            batch_file.seek(0)
            yield batch_file, batch_ids

        self.logger.info('  Prepared {} rows for import to {}'.format(total_rows, mapping['sf_object']))

    def _convert(self, value):
        if value:
            if isinstance(value, datetime.datetime):
                return value.isoformat()
            else:
                try:
                    return value.encode('utf8')
                except AttributeError:
                    pass

    def _init_db(self):
        # initialize the DB engine
        self.engine = create_engine(self.options['database_url'])

        # initialize DB metadata
        self.metadata = MetaData()
        self.metadata.bind = self.engine

        # initialize the automap mapping
        self.base = automap_base(bind=self.engine, metadata=self.metadata)
        self.base.prepare(self.engine, reflect=True)

        # Loop through mappings and reflect each referenced table
        self.tables = {}
        for name, mapping in self.mapping.items():
            if 'table' in mapping and mapping['table'] not in self.tables:
                self.tables[mapping['table']] = self.base.classes[mapping['table']]

        # initialize the DB session
        self.session = Session(self.engine)

    def _init_mapping(self):
        self.mapping = hiyapyco.load(
            self.options['mapping'],
            loglevel='INFO'
        )

def process_incoming_rows(f, record_type=None):
    for line in f:
        if record_type:
            yield line[:-1] + b',' + record_type + b'\n'
        else:
            yield line

def log_progress(iterable, logger, batch_size=10000):
    i = 0
    for x in iterable:
        yield x
        i += 1
        if not i % batch_size:
            logger.info('Processing... ({})'.format(i))
    logger.info('Done! Processed {} records'.format(i))

class QueryData(BaseSalesforceApiTask):
    task_options = {
        'database_url': {
            'description': 'A DATABASE_URL where the query output should be written',
            'required': True,
        },
        'mapping': {
            'description': 'The path to a yaml file containing mappings of the database fields to Salesforce object fields',
            'required': True,
        },
    }

    def _run_task(self):
        self._init_mapping()
        self._init_db()

        for mapping in self.mappings.values():
            soql = self._soql_for_mapping(mapping)
            self._run_query(soql, mapping)

    def _init_db(self):
        self.models = {}

        # initialize the DB engine
        self.engine = create_engine(self.options['database_url'])

        # initialize DB metadata
        self.metadata = MetaData()
        self.metadata.bind = self.engine

        # Create the tables
        self._create_tables()

        # initialize the automap mapping
        self.base = automap_base(bind=self.engine, metadata=self.metadata)
        self.base.prepare(self.engine, reflect=True)

        # Loop through mappings and reflect each referenced table
        self.tables = {}

        # initialize session
        self.session = create_session(bind=self.engine, autocommit=False)

    def _init_mapping(self):
        self.mappings = hiyapyco.load(
            self.options['mapping'],
            loglevel='INFO'
        )

    def _soql_for_mapping(self, mapping):
        sf_object = mapping['sf_object']
        fields = [field['sf'] for field in self._fields_for_mapping(mapping)]
        soql = "SELECT {fields} FROM {sf_object}".format(**{
            'fields': ', '.join(fields),
            'sf_object': sf_object,
        })
        if 'record_type' in mapping:
            soql += ' WHERE RecordType.DeveloperName = \'{}\''.format(mapping['record_type'])
        return soql

    def _run_query(self, soql, mapping):
        self.logger.info('Creating bulk job for: {sf_object}'.format(**mapping))
        job = self.bulk.create_query_job(mapping['sf_object'], contentType='CSV')
        self.logger.info('Job id: {0}'.format(job))
        self.logger.info('Submitting query: {}'.format(soql))
        batch = self.bulk.query(job, soql)
        self.logger.info('Batch id: {0}'.format(batch))
        self.bulk.wait_for_batch(job, batch)
        self.logger.info('Batch {0} finished'.format(batch))
        self.bulk.close_job(job)
        self.logger.info('Job {0} closed'.format(job))

        conn = self.session.connection()
        for result_file in self._get_results(batch, job):
            with conn.connection.cursor() as cursor:
                # Map column names
                reader = csv.reader(result_file)
                sf_header = reader.next()
                columns = []
                for sf in sf_header:
                    if sf == 'Records not found for this query':
                        continue
                    if sf == 'Id':
                        column = 'id'
                    else:
                        column = mapping['fields'].get(sf)
                        if not column:
                            column = mapping['lookups'][sf]['key_field']
                    columns.append(column)
                if not columns:
                    continue
                record_type = mapping.get('record_type')
                if record_type:
                    columns.append('record_type')
                processor = log_progress(
                    process_incoming_rows(result_file, record_type),
                    self.logger,
                )
                cursor.copy_expert(
                    'COPY {} ({}) FROM STDIN WITH (FORMAT CSV)'.format(
                        mapping['table'],
                        b','.join(columns),
                    ),
                    IteratorBytesIO(processor),
                )
                self.session.commit()

    def _get_results(self, batch_id, job_id):
        result_ids = self.bulk.get_query_batch_result_ids(batch_id, job_id=job_id)
        if not result_ids:
            raise RuntimeError('Batch is not complete')
        for result_id in result_ids:
            self.logger.info('Result id: {}'.format(result_id))
            uri = urlparse.urljoin(
                self.bulk.endpoint + "/",
                "job/{0}/batch/{1}/result/{2}".format(
                    job_id, batch_id, result_id),
            )
            with _download_file(uri, self.bulk) as f:
                self.logger.info('Result {} downloaded'.format(result_id))
                yield f

    def _create_tables(self):
        for name, mapping in self.mappings.items():
            self._create_table(mapping)
        self.metadata.create_all()

    def _fields_for_mapping(self, mapping):
        fields = []
        for sf_field, db_field in mapping.get('fields', {}).items():
            fields.append({ 'sf': sf_field, 'db': db_field })
        for sf_field, lookup in mapping.get('lookups', {}).items():
            fields.append({ 'sf': sf_field, 'db': lookup['key_field'] })
        return fields

    def _create_table(self, mapping):
        model_name = '{}Model'.format(mapping['table'])
        mapper_kwargs = {}
        table_kwargs = {}
        if mapping['table'] in self.models:
            mapper_kwargs['non_primary'] = True
            table_kwargs['extend_existing'] = True
        else:
            self.models[mapping['table']] = type(model_name, (object,), {})
        
        fields = []
        fields.append(Column('id', Unicode(255), primary_key=True))
        for field in self._fields_for_mapping(mapping):
            fields.append(Column(field['db'], Unicode(255)))
        if 'record_type' in mapping:
            fields.append(Column('record_type', Unicode(255)))
        t = Table(
            mapping['table'],
            self.metadata,
            *fields,
            **table_kwargs
        )

        mapper(self.models[mapping['table']], t, **mapper_kwargs)

@contextmanager
def _download_file(uri, bulk_api):
    resp = requests.get(uri, headers=bulk_api.headers(), stream=True)
    with tempfile.TemporaryFile('w+b') as f:
        for chunk in resp.iter_content(chunk_size=None):
            f.write(chunk)
        f.seek(0)
        yield f
