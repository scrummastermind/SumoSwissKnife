__version__ = "v0.0.1"

import json
import requests
import pprint
from .Utils import toTitle
import re

pp = pprint.PrettyPrinter(indent=4)


def pprint(content):
    pp.pprint(content)


class SumoAPIException(Exception):
    def __init__(self, dictionary):
        for key in dictionary:
            setattr(self, key, dictionary[key])


try:
    import cookielib
except ImportError:
    import http.cookiejar as cookielib


class SumoLogic(object):

    def __init__(self, accessId, accessKey, endpoint=None,
                 caBundle=None, cookieFile='cookies.txt'):
        self.session = requests.Session()
        self.session.auth = (accessId, accessKey)
        self.session.headers = {
                                'content-type': 'application/json',
                                'accept': 'application/json'
                                }
        if caBundle is not None:
            self.session.verify = caBundle
        cj = cookielib.FileCookieJar(cookieFile)
        self.session.cookies = cj
        if endpoint is None:
            raise(requests.exceptions.InvalidURL('Sumo Endpoint is not set'))
        else:
            self.endpoint = endpoint
        if self.endpoint[-1:] == "/":
            raise(requests.exceptions.InvalidURL(
                        'Sumo Endpoint should not end with a slash character'))

    def get_error(self, response):
        sumo_error = None
        errors_json_data = json.loads(response.text)
        sumo_error = {}
        error_id = errors_json_data['id']
        sumo_error['id'] = error_id
        sumo_error['status_code'] = response.status_code
        m = re.search('(?:v\d)\/([^\?]+)\?', response.url)
        resource_name = ''
        if m:
            resource_name = m.group(1)
        sumo_error['resource_name'] = toTitle(resource_name)
        request_errors = errors_json_data['errors']
        error_msg_errors = []
        for error in request_errors:
            current_error = {}
            current_error['code'] = toTitle(error['code'])
            current_error['message'] = error['message']
            error_msg_errors.append(current_error)
        sumo_error['errors'] = error_msg_errors

        formatted_error_msg=' - Error - {id}:\n'.format(id=error_id)
        formatted_error_msg += '   - Action: Gettirng or Manupulating {resource_name}\n'.format(resource_name=resource_name)

        for err in error_msg_errors:
            formatted_error_msg += "   - Reason: {code}\n   - Details: {message}".format(code=err['code'], message=err['message'])

        formatted_error_msg += '\n'

        sumo_error['msg'] = formatted_error_msg

        sumo_cxception = SumoAPIException(sumo_error)

        return sumo_cxception

    def delete(self, uri=None, params=None):
        r = self.session.delete(self.endpoint + uri, params=params)
        if 400 <= r.status_code < 600:
            error = self.get_error(r)
            raise error
        return r

    def get(self, uri=None, params=None):
        r = self.session.get(self.endpoint + uri, params=params)
        if 400 <= r.status_code < 600:
            error = self.get_error(r)
            raise error
        return r

    def post(self, uri=None, params=None, headers=None):
        r = self.session.post(self.endpoint + uri,
                              data=json.dumps(params), headers=headers)
        if 400 <= r.status_code < 600:
            error = self.get_error(r)
            raise error
        return r

    def put(self, uri=None, params=None, headers=None):
        r = self.session.put(self.endpoint + uri,
                             data=json.dumps(params), headers=headers)
        if 400 <= r.status_code < 600:
            error = self.get_error(r)
            raise error
        return r

    def get_resources(self,
                      method='get',
                      api_version='v1',
                      uri_name=None,
                      parent_uri_name=None,
                      uri_id=None,
                      parent_uri_id=None,
                      json_root='data',
                      results_format=None,
                      request_params=None):

        if not uri_name:
            raise(requests.exceptions.InvalidURL(
                                               'Sumo Endpoint URI is not set'))


        if request_params:
          if 'offset' not in request_params:
              request_params['offset'] = 0

          if 'limit' not in request_params:
              request_params['limit'] = 250
        else:
            request_params = {'offset': 0, 'limit': 250}


        uri_prefix = '/api/{api_version}'.format(api_version=api_version)
        uri = None
        uri_postfix = None

        uri_postfix = uri_name if uri_name else uri_postfix
        uri_postfix = '{uri_postfix}/{uri_id}'.format(
                      uri_postfix=uri_postfix, uri_id=uri_id) if\
            (uri_postfix and uri_id) else uri_postfix

        if (parent_uri_name and parent_uri_id and uri_prefix):
            uri_prefix = \
                '{uri_prefix}/{parent_uri_name}/{parent_uri_id}'.format(
                                            uri_prefix=uri_prefix,
                                            parent_uri_name=parent_uri_name,
                                            parent_uri_id=parent_uri_id)

        uri = '{uri_prefix}/{uri_postfix}'.\
            format(uri_prefix=uri_prefix, uri_postfix=uri_postfix)

        if not uri:
            raise(Exception("URI is invalid"))

        api_call_to_execute = getattr(self, method)

        r = api_call_to_execute(**{'uri': uri, 'params': request_params})

        return json.loads(r.text)
