import logging

from spaceone.core.manager import BaseManager
from spaceone.monitoring.error import *


_LOGGER = logging.getLogger(__name__)


class AWSManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_connector = self.locator.get_connector('HealthConnector')

    def verify(self, schema, options, secret_data):
        self.aws_connector.create_session(schema, options, secret_data)
