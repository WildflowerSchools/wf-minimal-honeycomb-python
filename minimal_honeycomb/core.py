from honeycomb import HoneycombClient
import json
import os

INDENT_STRING = '  '
class MinimalHoneycombClient(HoneycombClient):

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
        self.client = HoneycombClient(
            uri=uri,
            client_credentials={
                'token_uri': token_uri,
                'audience': audience,
                'client_id': client_id,
                'client_secret': client_secret,
            }
        )

    def request(
        self,
        request_type,
        request_name,
        arguments,
        return_object
    ):
        request_string = self.graphql_request_string(
            request_type,
            request_name,
            arguments,
            return_object
        )
        variables = {argument_name: argument_info['value'] for argument_name, argument_info in arguments.items()}
        results = self.client.raw_query(request_string, variables)
        return results

    def graphql_request_string(
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
        request_string = self.graphql_formatter(object)
        return request_string

    def graphql_formatter(self, object, indent_level=0):
        graphql_string = ''
        for object_component in object:
            if hasattr(object_component, 'keys'):
                if len(object_component) == 0:
                    raise ValueError('Object for formatting has zero length')
                if len(object_component) > 1:
                    raise ValueError('Multiple objects with children must be represented by separate dicts')
                # parent = object_component.keys()[0]
                # children = object_component.values()[0]
                for parent, children in object_component.items():
                    graphql_string += '{}{} {{\n{}{}}}\n'.format(
                        INDENT_STRING*indent_level,
                        parent,
                        self.graphql_formatter(children, indent_level=indent_level + 1),
                        INDENT_STRING*indent_level
                    )
            else:
                graphql_string += '{}{}\n'.format(
                    INDENT_STRING*indent_level,
                    object_component
                )
        return graphql_string
