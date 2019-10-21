__version__ = "v0.0.1"

import time
import logging
import sublime
import json

from .Utils import get_formatted_results
from .sumologic import SumoAPIException

from threading import Thread

logger = logging.getLogger(__name__)


class Command(object):
    timeout = 45

    def __init__(self, callback, sumo=None, api_call=None, params=None,
                 query=None, encoding='utf-8', options=None,
                 timeout=15, silenceErrors=False):
        if options is None:
            options = {}
        self.sumo = sumo
        self.api_call = api_call
        self.callback = callback
        self.params = params
        self.query = query
        self.encoding = encoding
        self.options = options
        self.timeout = timeout
        self.silenceErrors = silenceErrors

        if 'show_query' not in self.options:
            self.options['show_query'] = False
        elif self.options['show_query'] not in ['top', 'bottom']:
            self.options['show_query'] = 'top' if (
                isinstance(self.options['show_query'],
                           bool) and self.options['show_query']) else False

    def run(self):
        if not (self.sumo and self.api_call):
            return
        elif self.api_call == 'search' and not self.query:
            return

        resultString = ''
        offset = 0
        request_params = None

        try:
            if self.api_call:

                if self.params and hasattr(self.params, 'keys') and 'request_params' in self.params.keys():
                        request_params = self.params['request_params']

                next_token = None
                api_call_to_execute = getattr(self.sumo, self.api_call)
                json_raw_data = api_call_to_execute(
                        **self.params) if self.params else api_call_to_execute()

                if json_raw_data and 'next' in json_raw_data.keys():
                    next_token = json_raw_data['next']

                if request_params and hasattr(request_params, 'keys') and  'offset' in request_params.keys():
                    offset = request_params['offset']

                if next_token:
                    if self.params:
                        if request_params:
                            self.params['request_params']['token'] = next_token
                        else:
                            self.params['request_params'] = {'token': next_token}
                    else:
                        self.params = {'request_params': {'token': next_token}}
                else:
                    if request_params and hasattr(request_params, 'keys') and  'token' in request_params.keys():
                        del self.params['request_params']['token']

                json_root = None
                if self.params and 'json_root' in self.params.keys():
                    json_root = self.params['json_root']

                    if json_root and json_root in json_raw_data.keys():
                        json_raw_data = json_raw_data[json_root]

                results_format = self.params.get('results_format', 'grid')


                resultString = get_formatted_results(root=self.params['json_root'], results_format=results_format,
                                                     json_raw_data=json_raw_data,
                                                     offset=offset)

                if self.options['show_query']:
                    queryTimerStart, queryTimerEnd = 0
                    formattedQueryInfo = self._formatShowQuery(
                                   self.query, queryTimerStart, queryTimerEnd)

                    queryPlacement = self.options['show_query']

                    if queryPlacement == 'top':
                        resultString = "{0}\n{1}".format(
                                            formattedQueryInfo, resultString)
                    elif queryPlacement == 'bottom':
                        resultString = "{0}{1}\n".format(
                                         resultString, formattedQueryInfo)

        except SumoAPIException as e:
                resultString = []
                resultString.append(e.__dict__)
                # sublime.error_message(e.msg)

        self.callback(resultString, params=self.params)

    @staticmethod
    def _formatShowQuery(query, queryTimeStart, queryTimeEnd):
        resultInfo = "/*\n-- Executed querie(s) at {0} \
        took {1:.3f} s --".format(
            str(time.strftime("%Y-%m-%d %H:%M:%S",
                              time.localtime(queryTimeStart))),
            (queryTimeEnd - queryTimeStart)
            )

        resultLine = "-" * (len(resultInfo) - 3)
        resultString = "{0}\n{1}\n{2}\n{3}\n*/".format(
            resultInfo, resultLine, query, resultLine)
        return resultString

    @staticmethod
    def createAndRun(callback, sumo=None, api_call=None, params=None,
                     query=None, encoding='utf-8', options=None,
                     timeout=15, silenceErrors=False):
        if options is None:
            options = {}

        command = Command(callback=callback, sumo=sumo,
                          api_call=api_call, params=params,
                          query=query, encoding=encoding,
                          options=options, timeout=timeout,
                          silenceErrors=silenceErrors)
        command.run()


class ThreadCommand(Command, Thread):
    def __init__(self, callback, sumo=None, api_call=None, params=None,
                 query=None, encoding='utf-8', options=None,
                 timeout=Command.timeout, silenceErrors=False):
        if options is None:
            options = {}

        Command.__init__(self, callback=callback, sumo=sumo,
                         api_call=api_call, params=params,
                         query=query, encoding=encoding, options=options,
                         timeout=timeout, silenceErrors=silenceErrors)
        Thread.__init__(self)

    @staticmethod
    def createAndRun(callback, sumo=None, api_call=None, params=None,
                     query=None, encoding='utf-8', options=None,
                     timeout=Command.timeout, silenceErrors=False):
        if options is None:
            options = {}

        command = ThreadCommand(callback=callback, sumo=sumo,
                                api_call=api_call, params=params, query=query,
                                encoding=encoding, options=options,
                                timeout=timeout, silenceErrors=silenceErrors)
        command.start()
