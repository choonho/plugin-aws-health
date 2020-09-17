__all__ = ['MonitoringManager']

import logging

from spaceone.core.error import *
from spaceone.core.manager import BaseManager

_LOGGER = logging.getLogger(__name__)


class MonitoringManager(BaseManager):
    def __init__(self, transaction):
        super().__init__(transaction)

    def list_resources(self, schema, options, secret_data, filters, resource, start, end, sort, limit):
        # call ec2 connector
        connector = self.locator.get_connector('HealthConnector')

        # make query, based on options, secret_data, filter
        query = filters

        return connector.collect_info(schema, query, options, secret_data, start, end, resource, sort, limit)
