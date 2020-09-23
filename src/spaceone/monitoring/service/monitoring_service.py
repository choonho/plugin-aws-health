# -*- coding: utf-8 -*-

import logging

from datetime import datetime
from datetime import timedelta

from spaceone.core.error import *
from spaceone.core.service import *
from spaceone.monitoring.error import *


_LOGGER = logging.getLogger(__name__)
DEFAULT_SCHEMA = 'aws_access_key'

FILTER_FORMAT = [
]

NUM_OF_LIMIT = 30


@authentication_handler
class MonitoringService(BaseService):
    def __init__(self, metadata):
        super().__init__(metadata)

    @transaction
    @check_required(['options', 'secret_data', 'filter', 'start', 'end'])
    @change_timestamp_value(['start', 'end'], timestamp_format='iso8601')
    def list_resources(self, params):
        """ Get quick list of resources

        Args:
            params (dict) {
                'schema': 'str',
                'options': 'dict',
                'secret_data': 'dict',
                'filter': 'dict',
                'resource': 'str',
                'start': 'timestamp',
                'end': 'timestamp',
                'sort': 'dict',
                'limit': 'int'
            }

        Returns: list of resources
        """
        manager = self.locator.get_manager('MonitoringManager')
        schema = params.get('schema', DEFAULT_SCHEMA)
        options = params['options']
        secret_data = params['secret_data']
        filters = params['filter']
        resource = params.get('resource', None)
        start = params.get('start', datetime.utcnow() - timedelta(days=1))
        end = params.get('end', datetime.utcnow())
        sort = params.get('sort', None)
        limit = params.get('limit', NUM_OF_LIMIT)

        if options == {}:
            options = {'eventStatusCodes':['open', 'upcoming', 'closed'],
                       'all_events': True}

        if start > end:
            start = end

        return manager.list_resources(schema, options, secret_data, filters, resource, start, end, sort, limit)

