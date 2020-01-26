from gqlpycgen.client import Client, FileUpload
from uuid import uuid4
import json
import os
import logging

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

    def fetch_data(
        self,
        request_name,
        arguments=None,
        return_data=None,
        data_id_field_name=None,
        read_chunk_size=100,
        sort_arguments=None
    ):
        if arguments == None:
            arguments = dict()
        if 'page' in arguments.keys():
            raise ValueError('Specifying pagination parameters is redundant. Use read_chunk_size and sort_arguments')
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
            page_argument = {
                'page': {
                    'type': 'PaginationInput',
                    'value': {
                        'max': read_chunk_size,
                        'cursor': cursor,
                        'sort': sort_arguments
                    }
                }
            }
            arguments_with_pagination_details = {**arguments, **page_argument}
            logger.info('Sending request {}'.format(request_index))
            result = self.request(
                request_type='query',
                request_name=request_name,
                arguments=arguments_with_pagination_details,
                return_object=return_object
            )
            try:
                returned_data = result['data']
                count=result['page_info']['count']
                cursor=result['page_info']['cursor']
            except:
                raise ValueError('Received unexpected result from Honeycomb:\n{}'.format(result))
            try:
                num_data_points=len(returned_data)
            except:
                raise ValueError('Expected list for data. Received {}'.format(returned_data))
            if num_data_points != count:
                raise ValueError('Honeycomb reported count as {} but received {} data points'.format(
                    count,
                    num_data_points
                ))
            if num_data_points == 0:
                logger.info('Request {} returned no data points. Terminating fetch.'.format(request_index))
                break
            new_data_point_count = 0
            for datum in returned_data:
                try:
                    datum_id = datum[data_id_field_name]
                except:
                    raise ValueError('Returned datum does not contain field {}'.format(data_id_field_name))
                if datum_id not in data_ids:
                    new_data_point_count += 1
                    data_ids.add(datum_id)
                    data_list.append(datum)
            logger.info('Request {} returned {} data points containing {} new data points'.format(
                request_index,
                num_data_points,
                new_data_point_count
            ))
            if cursor is None:
                logger.info('No cursor returned. Terminating fetch')
                break
            request_index += 1
        logger.info('Fetched {} data points total'.format(len(data_list)))
        return data_list

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

    def parse_datapoints(
        self,
        datapoints
    ):
        logger.info('Parsing {} datapoints'.format(len(datapoints)))
        data=[]
        for datapoint in datapoints:
            data_blob = datapoint.get('file', {}).get('data')
            if data_blob is not None:
                parsed_data_dict_list = self.parse_data_blob(data_blob)
                del datapoint['file']['data']
                for parsed_data_dict in parsed_data_dict_list:
                    parsed_data = {'parsed_data': parsed_data_dict}
                    data.append({**datapoint, **parsed_data})
            else:
                data.append(datapoint)
        return data

    def parse_data_blob(
        self,
        data_blob
    ):
        data_dict_list=[]
        if isinstance(data_blob, dict):
            data_dict_list.append(data_blob)
            return data_dict_list
        if isinstance(data_blob, list):
            for item in data_blob:
                data_dict_list.extend(self.parse_data_blob(item))
            return data_dict_list
        try:
            data_dict_list.extend(self.parse_data_blob(json.loads(data_blob)))
            return data_dict_list
        except:
            pass
        try:
            for line in data_blob.split('\n'):
                if len(line) > 0:
                    data_dict_list.extend(self.parse_data_blob(line))
            return data_dict_list
        except:
            pass
        return data_dict_list
