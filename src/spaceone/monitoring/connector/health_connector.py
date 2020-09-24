import boto3
import re
import logging
import sys

from botocore.exceptions import ClientError

from multiprocessing import Pool
from datetime import datetime

from spaceone.core import utils
from spaceone.core.transaction import Transaction
from spaceone.core.error import *
from spaceone.core.connector import BaseConnector

from spaceone.monitoring.error import *

__all__ = ["HealthConnector"]


_LOGGER = logging.getLogger(__name__)
DEFAULT_REGION = 'us-east-1'
NUMBER_OF_CONCURRENT = 1


class HealthConnector(BaseConnector):

    def __init__(self, transaction, config):
        super().__init__(transaction, config)

    def create_session(self, schema, options, secret_data):
        """ Verify health Session
        """
        create_session(schema, secret_data, options)

    def collect_info(self, schema, query, options, secret_data, start, end, resource, sort, limit=200):
        """
        Args:
            query (dict): example
                  {
                      'instance_id': ['i-123', 'i-2222', ...]
                      'instance_type': 'm4.xlarge',
                      'region_name': ['aaaa']
                  }
            resource: arn:aws:ec2:<REGION>:<ACCOUNT_ID>:instance/<instance-id>
        If there is region_name in query, this indicates searching only these regions
        """
        (query, resource_ids, region_name) = self._check_query(query)
        post_filter_cache = False if len(region_name) > 0 else True

        try:
            (resource_ids, regions) = _parse_arn(resource)
            print(resource_ids)
            print(regions)
        except Exception as e:
            _LOGGER.error(f'[collect_info] fail to parse arn:{e}')

        params = []
        region_name_list = []               # For filter_cache
        # health is global API
        regions = [DEFAULT_REGION]
        for region in regions:
            params.append({
                'schema': schema,
                'region_name': region,
                'query': query,
                'options': options,
                'resource_ids': resource_ids,
                'secret_data': secret_data,
                'start': start,
                'end': end,
                'sort': sort,
                'limit': limit
            })

        with Pool(NUMBER_OF_CONCURRENT) as pool:

            result = pool.map(discover_health, params)
            no_result = True
            for resources in result:
                (collected_resources, region_name) = resources
                if len(collected_resources) > 0:
                    region_name_list.append(region_name)
                    try:
                        no_result = False
                        response = _prepare_response_schema()
                        response['result'] = {'logs': collected_resources}
                        yield response
                    except Exception as e:
                        _LOGGER.error(f'[collect_info] skip return {resource}, {e}')
            if no_result:
                response = _prepare_response_schema()
                response['result'] = {'logs': []}
                yield response

    @staticmethod
    def _check_query(query):
        resource_ids = []
        filters = []
        region_name = []
        for key, value in query.items():
            if key == 'instance_id' and isinstance(value, list):
                resource_ids = value

            elif key == 'region_name' and isinstance(value, list):
                region_name.extend(value)

            else:
                if not isinstance(value, list):
                    value = [value]

                if len(value) > 0:
                    filters.append({'Name': key, 'Values': value})

        return (filters, resource_ids, region_name)

#######################
# AWS Boto3 session
#######################
def create_session(schema, secret_data: dict, options={}):
    _check_secret_data(secret_data)

    aws_access_key_id = secret_data['aws_access_key_id']
    aws_secret_access_key = secret_data['aws_secret_access_key']
    role_arn = secret_data.get('role_arn')

    if schema:
        return getattr(sys.modules[__name__], f'_create_session_{schema}')(aws_access_key_id,
                                                                           aws_secret_access_key,
                                                                           role_arn)
    else:
        raise ERROR_REQUIRED_CREDENTIAL_SCHEMA()


def _check_secret_data(secret_data):
    if 'aws_access_key_id' not in secret_data:
        raise ERROR_REQUIRED_PARAMETER(key='secret.aws_access_key_id')

    if 'aws_secret_access_key' not in secret_data:
        raise ERROR_REQUIRED_PARAMETER(key='secret.aws_secret_access_key')


def _create_session_aws_access_key(aws_access_key_id, aws_secret_access_key, role_arn=None):
    return boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


def _create_session_aws_assume_role(aws_access_key_id, aws_secret_access_key, role_arn):
    session = _create_session_aws_access_key(aws_access_key_id, aws_secret_access_key)

    sts = session.client('sts')
    assume_role_object = sts.assume_role(RoleArn=role_arn, RoleSessionName=utils.generate_id('AssumeRoleSession'))
    credentials = assume_role_object['Credentials']

    return boto3.Session(aws_access_key_id=credentials['AccessKeyId'],
                         aws_secret_access_key=credentials['SecretAccessKey'],
                         aws_session_token=credentials['SessionToken'])


def _set_connect(schema, secret_data, region_name, service="health"):
    """
    """
    session = create_session(schema, secret_data)
    return session.client(service, region_name=region_name)


def discover_health(params):
    """
    Args: params (dict): {
                'schema': 'str,
                'region_name': 'str',
                'query': 'dict',
                'options': 'dict',
                'resource_ids': 'list'
                'secret_data': 'dict',
                'start': 'datetime',
                'end': 'datetime',
                'sort': 'dict',
                'limit': 'int'
            }

    Returns: Resources, region_name
    """
    print(f'[discover_health] {params["region_name"]}')
    client = _set_connect(params['schema'], params['secret_data'], params['region_name'])
    try:
        resources = _lookup_events(client, params)
        return resources
    except ERROR_SUBSCRIPTION_REQUIRED as e:
        raise ERROR_SUBSCRIPTION_REQUIRED()
    except Exception as e:
        _LOGGER.error(f'[discover_health] skip region: {params["region_name"]}, {e}')
    return [], params['region_name']


def _lookup_events(client, params):
    events = []
    resource_list = []
    event_query = {}
    filter_query = {}
    region_name = params['region_name']
    options = params.get('options', {})
    all_events = options.get('all_events', False)

    if 'eventStatusCodes' in options:
        filter_query.update({'eventStatusCodes': options['eventStatusCodes']})
    else:
        filter_query.update({'eventStatusCodes': ['open', 'upcoming']})

    filter_query.update({'startTimes': [{'from': params['start'], 'to': params['end']}]})

    # Paginator config
    limit = params.get('limit')
    print(f'limit: {limit}')
    page_size = limit if limit < 50 else 50
    event_query.update({'filter': filter_query, 'PaginationConfig': {'MaxItems': limit, 'PageSize': page_size}})
    try:
        print(event_query)
        paginator = client.get_paginator('describe_events')
        response_iterator = paginator.paginate(**event_query)
        for response in response_iterator:
            events.extend(response['events'])
        if len(events) == 0:
            # Fast return if No resources
            print("No events")
            return events, region_name

    except ClientError as e:
        print('=' * 30)
        print(e.response['Error']['Code'])
        print('=' * 30)
        if e.response['Error']['Code'] == 'SubscriptionRequiredException':
            raise ERROR_SUBSCRIPTION_REQUIRED()
        else:
            print(e)

    except Exception as e:
        print(f'[_lookup_events] Fail to lookup health events: {e}')
        return resource_list, region_name

    # Find Events
    for event in events:
        try:
            result = _parse_health_event(event)
            entity_count = _find_affected_entities(client, result['arn'])
            if entity_count > 0:
                result['count'] = entity_count
                result['reference'] = _add_reference(result['arn'])
                resource_list.append(result)
            elif all_events is True:
                result['count'] = 0
                result['reference'] = _add_reference(result['arn'])
                resource_list.append(result)

        except Exception as e:
            print(f'[_lookup_events] error {e}')

    return resource_list, region_name


def _add_reference(eventArn):
    reference = {
        'resource_id': eventArn,
        'external_link': f'https://phd.aws.amazon.com/phd/home#/event-log?eventID={eventArn}&eventTab=details&layout=vertical'
    }
    return reference


def _find_affected_entities(client, eventArn):
    filter_query = {
        'eventArns': [eventArn]
    }
    try:
        resp = client.describe_entity_aggregates(**filter_query)
        if 'entityAggregates' in resp:
            return resp['entityAggregates'][0].get('count', 0)
        return 0
    except Exception as e:
        _LOGGER.debug(f'[_find_affected_entities] failed {e}')
        return 0


def _parse_health_event(health_event):
    """ Parse health Event

    Args: healthEvent (raw data)
    Returns: dict
    """
    result = {}
    wanted_items = ['service', 'arn', 'eventTypeCode', 'eventTypeCategory', 'region', 'availabilityZone', 'startTime', 'endTime',
                    'lastUpdatedTime', 'statusCode']
    for item in wanted_items:
        if item in health_event:
            if isinstance(health_event[item], datetime):
                result[item] = health_event[item].isoformat()
            else:
                result[item] = health_event[item]

    #print(f'parse cloud trail event: {result}')
    return result


def _parse_arn(arn):
    """
    ec2)  arn:aws:ec2:<REGION>:<ACCOUNT_ID>:instance/<instance-id>

    arn:partition:service:region:account-id:resource-id
    arn:partition:service:region:account-id:resource-type/resource-id
    arn:partition:service:region:account-id:resource-type:resource-id

    Returns: resource_list, [regions]
    """
    p = (r"(?P<arn>arn):"
         r"(?P<partition>aws|aws-cn|aws-us-gov):"
         r"(?P<service>[A-Za-z0-9_\-]*):"
         r"(?P<region>[A-Za-z0-9_\-]*):"
         r"(?P<account>[A-Za-z0-9_\-]*):"
         r"(?P<resources>[A-Za-z0-9_\-:/]*)")
    r = re.compile(p)
    match = r.match(arn)
    if match:
        d = match.groupdict()
    else:
        return (None, None)
    region = d.get('region', None)
    resource_id = None
    resources = d.get('resources', None)
    if resources:
        items = re.split('/|:', resources)
        if len(items) == 1:
            resource_id = items[0]
        elif len(items) == 2:
            resource_type = items[0]
            resource_id = items[1]
        else:
            print(f'ERROR parsing: {resources}')
    return [resource_id], [region]


def _prepare_response_schema() -> dict:
    return {
        'resource_type': 'monitoring.Log',
        'actions': [
            {
                'method': 'process'
            }],
        'result': {}
    }


if __name__ == "__main__":
    import os

    aki = os.environ.get('AWS_ACCESS_KEY_ID', "<YOUR_AWS_ACCESS_KEY_ID>")
    sak = os.environ.get('AWS_SECRET_ACCESS_KEY', "<YOUR_AWS_SECRET_ACCESS_KEY>")

    secret_data = {
        #        'region_name': 'ap-northeast-2',
        'aws_access_key_id': aki,
        'aws_secret_access_key': sak
    }
    conn = HealthConnector(Transaction(), secret_data)
    #opts = conn.verify({}, secret_data)
    #print(opts)
    options = {'eventStatusCodes': ['open', 'upcoming', 'closed'],
               'all_events': True}
    query = {}
    #query = {'region_name': ['ap-northeast-2', 'us-east-1']}
    #query = {}
    from datetime import datetime, timedelta
    end = datetime.utcnow()
    start = end - timedelta(days=7)
    ec2_arn = 'arn:aws:ec2:ap-northeast-2:072548720675:instance/i-08c5592e084b24e20'
    sort = ""
    limit = 50
    resource_stream = conn.collect_info(schema='aws_access_key', query=query, options=options, secret_data=secret_data,
                                        start=start, end=end, resource=ec2_arn, sort=sort, limit=limit)
    import pprint
    for resource in resource_stream:
        pprint.pprint(resource)
