from gqlpycgen.client import Client, FileUpload
from uuid import uuid4
import datetime
import math
import json
import os
import logging
import time

logger = logging.getLogger(__name__)

INDENT_STRING = '  '

class MinimalHoneycombClient:
    def __init__(
        self,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    ):
        if uri is None:
            uri = os.getenv('HONEYCOMB_URI')
            if uri is None:
                raise ValueError('Honeycomb URI not specified and environment variable HONEYCOMB_URI not set')
        if token_uri is None:
            token_uri = os.getenv('HONEYCOMB_TOKEN_URI')
            if token_uri is None:
                raise ValueError('Honeycomb token URI not specified and environment variable HONEYCOMB_TOKEN_URI not set')
        if audience is None:
            audience = os.getenv('HONEYCOMB_AUDIENCE')
            if audience is None:
                raise ValueError('Honeycomb audience not specified and environment variable HONEYCOMB_AUDIENCE not set')
        if client_id is None:
            client_id = os.getenv('HONEYCOMB_CLIENT_ID')
            if client_id is None:
                raise ValueError('Honeycomb client ID not specified and environment variable HONEYCOMB_CLIENT_ID not set')
        if client_secret is None:
            client_secret = os.getenv('HONEYCOMB_CLIENT_SECRET')
            if client_secret is None:
                raise ValueError('Honeycomb client secret not specified and environment variable HONEYCOMB_CLIENT_SECRET not set')
        self.client = Client(
            uri=uri,
            client_credentials={
                'token_uri': token_uri,
                'audience': audience,
                'client_id': client_id,
                'client_secret': client_secret,
            }
        )

    def bulk_query(
        self,
        request_name,
        arguments=None,
        return_data=None,
        id_field_name=None,
        chunk_size=100,
        sort_arguments=None
    ):
        overall_start = time.time()
        if arguments == None:
            arguments = dict()
        if 'page' in arguments.keys():
            raise ValueError('Specifying pagination parameters is redundant. Use chunk_size and sort_arguments')
        return_object = [
            {'data': return_data},
            {'page_info': [
                'count',
                'cursor'
            ]}
        ]
        cursor = None
        data_list = list()
        data_ids = set()
        request_index = 0
        while True:
            bulk_query_iteration_start = time.time()
            page_argument = {
                'page': {
                    'type': 'PaginationInput',
                    'value': {
                        'max': chunk_size,
                        'cursor': cursor,
                        'sort': sort_arguments
                    }
                }
            }
            arguments_with_pagination_details = {**arguments, **page_argument}
            logger.info('Sending query request {}'.format(request_index))
            request_start = time.time()
            result = self.request(
                request_type='query',
                request_name=request_name,
                arguments=arguments_with_pagination_details,
                return_object=return_object
            )
            request_time = time.time() - request_start
            try:
                returned_data = result['data']
                count=result['page_info']['count']
                cursor=result['page_info']['cursor']
            except:
                raise ValueError('Received unexpected result from Honeycomb:\n{}'.format(result))
            try:
                num_data_items=len(returned_data)
            except:
                raise ValueError('Expected list for data. Received {}'.format(returned_data))
            if num_data_items != count:
                raise ValueError('Honeycomb reported count as {} but received {} data points'.format(
                    count,
                    num_data_items
                ))
            if num_data_items == 0:
                logger.info('Query request {} returned no data. Terminating fetch.'.format(request_index))
                break
            new_data_item_count = 0
            duplicate_check_start = time.time()
            for datum in returned_data:
                try:
                    datum_id = datum[id_field_name]
                except:
                    raise ValueError('Returned datum does not contain field {}'.format(id_field_name))
                if datum_id not in data_ids:
                    new_data_item_count += 1
                    data_ids.add(datum_id)
                    data_list.append(datum)
            duplicate_check_time = time.time() - duplicate_check_start
            bulk_query_iteration_time = time.time() - bulk_query_iteration_start
            logger.info('Query request {} returned {} data items containing {} new data items in {:.1f} ms (request operation {:.1f} ms, checking for duplicates {:.1f} ms)'.format(
                request_index,
                num_data_items,
                new_data_item_count,
                bulk_query_iteration_time*1000,
                request_time*1000,
                duplicate_check_time*1000
            ))
            if cursor is None:
                logger.info('No cursor returned. Terminating fetch')
                break
            request_index += 1
        overall_time = time.time() - overall_start
        logger.info('Bulk query returned {} data items total in {:.3f} seconds'.format(
            len(data_list),
            overall_time
        ))
        return data_list

    def bulk_mutation(
        self,
        request_name,
        arguments,
        return_object,
        chunk_size=100
    ):
        # Scan arguments to determine structure
        num_mutations = 1
        argument_is_list = dict()
        for argument_name, argument_info in arguments.items():
            try:
                num_argument_values = len(argument_info['value'])
                argument_is_list[argument_name] = True
                if num_mutations != 1 and num_argument_values != num_mutations:
                    raise ValueError('All argument values that are not singletons must be the same length')
                num_mutations = num_argument_values
            except:
                argument_is_list[argument_name] = False
        logger.info('Preparing to request {} mutations using endpoint {}'.format(
            num_mutations,
            request_name
        ))
        num_chunks = math.ceil(num_mutations/chunk_size)
        logger.info('Requesting mutations in {} chunks'.format(num_chunks))
        result_list=list()
        for chunk_index in range(num_chunks):
            logger.info('Sending chunk {}'.format(chunk_index))
            mutation_index_start = chunk_index*chunk_size
            mutation_index_end = min([(chunk_index + 1)*chunk_size, num_mutations])
            child_request_list = list()
            for mutation_index in range(mutation_index_start, mutation_index_end):
                child_arguments = dict()
                for argument_name, is_list in argument_is_list.items():
                    child_arguments[argument_name] = dict()
                    child_arguments[argument_name]['type'] = arguments[argument_name]['type']
                    if is_list:
                        child_arguments[argument_name]['value'] = arguments[argument_name]['value'][mutation_index]
                    else:
                        child_arguments[argument_name]['value'] = arguments[argument_name]['value']
                child_request = {
                    'name': request_name,
                    'arguments': child_arguments,
                    'return_object_name': 'return_object',
                    'return_object': return_object
                }
                child_request_list.append(child_request)
            result = self.compound_request(
                parent_request_type='mutation',
                parent_request_name=request_name,
                child_request_list=child_request_list
            )
            result_list.extend(list(result.values()))
        return result_list

    def request(
        self,
        request_type,
        request_name,
        arguments,
        return_object
    ):
        request_string = self.request_string(
            request_type,
            request_name,
            arguments,
            return_object
        )
        if arguments is not None:
            variables = {argument_name: argument_info['value'] for argument_name, argument_info in arguments.items()}
        else:
            variables = None
        if request_name == 'createDatapoint':
            # Prepare upload package
            filename = uuid4().hex
            try:
                data = variables.get('datapoint').get('file').get('data')
            except:
                raise ValueError('createDatapoint arguments do not contain datapoint.file.data field')
            try:
                content_type = variables.get('datapoint').get('file').get('contentType')
            except:
                raise ValueError('createDatapoint arguments do not contain datapoint.file.contentType field')
            files = FileUpload()
            data_json = json.dumps(data)
            files.add_file("variables.datapoint.file.data", filename, data_json, content_type)
            # Replace data with filename
            variables['datapoint']['file']['data'] = filename
            response = self.client.execute(request_string, variables, files)
        else:
            response = self.client.execute(request_string, variables)
        try:
            return_value = response[request_name]
        except:
            raise ValueError('Received unexpected response from Honeycomb: {}'.format(response))
        return return_value

    def request_string(
        self,
        request_type,
        request_name,
        arguments,
        return_object
    ):
        if arguments is not None:
            top_level_argument_list_string = ', '.join(['${}: {}'.format(argument_name, argument_info['type']) for argument_name, argument_info in arguments.items()])
            top_level_string = '{} {}({})'.format(
                request_type,
                request_name,
                top_level_argument_list_string
            )
            second_level_argument_list_string = ', '.join(['{}: ${}'.format(argument_name, argument_name) for argument_name in arguments.keys()])
            second_level_string = '{}({})'.format(
                request_name,
                second_level_argument_list_string
            )
        else:
            top_level_string = '{} {}'.format(
                request_type,
                request_name
            )
            second_level_string = request_name
        object = [
            {top_level_string: [
                {second_level_string: return_object}
            ]}
        ]
        request_string = self.request_string_formatter(object)
        return request_string

    def variables_string(
        self,
        request_type,
        request_name,
        arguments,
        return_object
    ):
        if arguments is not None:
            variables = {argument_name: argument_info['value'] for argument_name, argument_info in arguments.items()}
        else:
            variables = None
        if request_name == 'createDatapoint':
            # Prepare upload package
            filename = uuid4().hex
            try:
                data = variables.get('datapoint').get('file').get('data')
            except:
                raise ValueError('createDatapoint arguments do not contain datapoint.file.data field')
            try:
                content_type = variables.get('datapoint').get('file').get('contentType')
            except:
                raise ValueError('createDatapoint arguments do not contain datapoint.file.contentType field')
            files = FileUpload()
            data_json = json.dumps(data)
            files.add_file("variables.datapoint.file.data", filename, data_json, content_type)
            # Replace data with filename
            variables['datapoint']['file']['data'] = filename
        variables_string = json.dumps(variables, indent=4)
        return variables_string

    def compound_request(
        self,
        parent_request_type,
        parent_request_name,
        child_request_list
    ):
        request_string = self.compound_request_string(
            parent_request_type,
            parent_request_name,
            child_request_list
        )
        variables = dict()
        files = FileUpload()
        for child_request_index, child_request in enumerate(child_request_list):
            child_request_name = child_request['name']
            child_arguments = child_request['arguments']
            if child_arguments is not None:
                child_variables = dict()
                for child_argument_name, child_argument_info in child_arguments.items():
                    # print(child_argument_name)
                    # print(child_argument_info['value'])
                    child_variables[child_argument_name] = child_argument_info['value']
                if child_request_name == 'createDatapoint':
                    # Prepare upload package
                    filename = uuid4().hex
                    # print(child_variables)
                    try:
                        data = child_variables.get('datapoint').get('file').get('data')
                    except:
                        raise ValueError('createDatapoint arguments do not contain datapoint.file.data field')
                    try:
                        content_type = child_variables.get('datapoint').get('file').get('contentType')
                    except:
                        raise ValueError('createDatapoint arguments do not contain datapoint.file.contentType field')
                    data_json = json.dumps(data)
                    files.add_file(
                        'variables.datapoint_{}.file.data'.format(child_request_index),
                        filename,
                        data_json,
                        content_type
                    )
                    # Replace data with filename
                    child_variables['datapoint']['file']['data'] = filename
                for child_variable_name, child_variable_value in child_variables.items():
                    variables['{}_{}'.format(child_variable_name, child_request_index)] = child_variable_value
        response = self.client.execute(request_string, variables, files)
        try:
            return_value = response
        except:
            raise ValueError('Received unexpected response from Honeycomb: {}'.format(response))
        return return_value

    def compound_request_string(
        self,
        parent_request_type,
        parent_request_name,
        child_request_list
    ):
        num_child_requests = len(child_request_list)
        top_level_argument_string_list = []
        child_string_list = []
        for child_request_index, child_request in enumerate(child_request_list):
            child_request_name = child_request['name']
            child_arguments = child_request['arguments']
            child_return_object_name = child_request['return_object_name']
            child_return_object = child_request['return_object']
            if child_arguments is not None:
                child_argument_string_list=[]
                for argument_name, argument_info in child_arguments.items():
                    top_level_argument_string_list.append('${}_{}: {}'.format(
                        argument_name,
                        child_request_index,
                        argument_info['type']
                    ))
                    child_argument_string_list.append('{}: ${}_{}'.format(
                        argument_name,
                        argument_name,
                        child_request_index
                    ))
                child_argument_list_string = ', '.join(child_argument_string_list)
                child_request['child_string'] = '{}_{}: {}({})'.format(
                    child_return_object_name,
                    child_request_index,
                    child_request_name,
                    child_argument_list_string
                )
            else:
                child_request['child_string'] = '{}_{}: {}'.format(
                    child_return_object_name,
                    child_request_index,
                    child_request_name
                )
            if len(top_level_argument_string_list) > 0:
                top_level_argument_list_string = ', '.join(top_level_argument_string_list)
                top_level_string = '{} {}({})'.format(
                    parent_request_type,
                    parent_request_name,
                    top_level_argument_list_string
                )
            else:
                top_level_string = '{} {}'.format(
                    parent_request_type,
                    parent_request_name
                )
        object = [
            {top_level_string: [{child_request['child_string']: child_request['return_object']} for child_request in child_request_list]}
        ]
        request_string = self.request_string_formatter(object)
        return request_string

    def request_string_formatter(self, object, indent_level=0):
        request_string = ''
        for object_component in object:
            if hasattr(object_component, 'keys'):
                if len(object_component) == 0:
                    raise ValueError('Object for formatting has zero length')
                if len(object_component) > 1:
                    raise ValueError('Multiple objects with children must be represented by separate dicts')
                # parent = object_component.keys()[0]
                # children = object_component.values()[0]
                for parent, children in object_component.items():
                    request_string += '{}{} {{\n{}{}}}\n'.format(
                        INDENT_STRING*indent_level,
                        parent,
                        self.request_string_formatter(children, indent_level=indent_level + 1),
                        INDENT_STRING*indent_level
                    )
            else:
                request_string += '{}{}\n'.format(
                    INDENT_STRING*indent_level,
                    object_component
                )
        return request_string

def extract_assignment(
    assignments,
    start_time=None,
    end_time=None
):
    filtered_assignments = filter_assignments(
        assignments=assignments,
        start_time=start_time,
        end_time=end_time
    )
    if len(filtered_assignments) == 0:
        raise ValueError('No assignment matches the specified start and end times')
    if len(filtered_assignments) > 1:
        raise ValueError('Multiple assignments match the specified start and end times')
    return filtered_assignments[0]

def filter_assignments(
    assignments,
    start_time=None,
    end_time=None
):
    filtered_assignments = list()
    for assignment in assignments:
        assignment_start_time = from_honeycomb_datetime(assignment.get('start'))
        assignment_end_time = from_honeycomb_datetime(assignment.get('end'))
        if start_time is not None and assignment_end_time is not None and (start_time > assignment_end_time):
            continue
        if end_time is not None and assignment_start_time is not None and (end_time < assignment_start_time):
            continue
        filtered_assignments.append(assignment)
    return filtered_assignments

def from_honeycomb_datetime(honeycomb_datetime):
    if honeycomb_datetime is None:
        return None
    return datetime.datetime.strptime(honeycomb_datetime, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=datetime.timezone.utc)

def to_honeycomb_datetime(python_datetime):
    if python_datetime is None:
        return None
    return python_datetime.astimezone(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
