__version__ = "v0.0.1"

import logging
from . import Command as C
from .sumologic import SumoLogic
from .Utils import merge_dicts

logger = logging.getLogger(__name__)


class Connection(object):
    name = None
    accessId = None
    accessKey = None
    endpoint = None
    settings = None
    encoding = None
    show_query = None
    history = None
    timeout = None
    sumo = None
    collectors = None

    def __init__(self, name, options, settings=None,
                 commandClass='ThreadCommand'):
        self.name = name

        if settings is None:
            settings = {}

        self.settings = settings
        self.encoding = 'utf-8'
        self.accessId = options.get('accessId', None)
        self.accessKey = options.get('accessKey', None)
        self.endpoint = options.get('endpoint', None)
        self.show_query = settings.get('show_query', False)
        self.useStreams = settings.get('use_streams', False)

        sumo_endpoint = self.getSumoAPIEndPoint()

        self.sumo = SumoLogic(self.accessId, self.accessKey, sumo_endpoint)
        self.Command = getattr(C, commandClass)

    def __str__(self):
        return self.name

    def info(self):
        return 'Sumo Connection: {accessId} @ {name}'.format(
            accessId=self.accessId, name=self.name)

    def getSumoAPIEndPoint(self):
        protocol = 'http' if 'local' in self.endpoint else 'https'
        full_endpoint = '{protocol}://{endpoint}'.format(
                                    protocol=protocol, endpoint=self.endpoint)
        return full_endpoint

    def runInternalNamedQueryCommand(self, api_call, params, callback):
        def cb(result, params):
            callback(result, params)

        self.Command.createAndRun(callback=cb, sumo=self.sumo,
                                  api_call=api_call, params=params,
                                  encoding=self.encoding,
                                  options={'show_query': self.show_query},
                                  timeout=self.timeout, silenceErrors=False)

    def getCollectors(self, callback, params=None):
        local_params = {"uri_name": "collectors", "json_root": "collectors",
                        "results_format": "json"}
        self.runInternalNamedQueryCommand(
          api_call="get_resources", params= merge_dicts(master=params, slave=local_params),
          callback=callback)

    def getSources(self, collector_id, callback, params=None):
        local_params = {"uri_name": "sources", "parent_uri_name": "collectors",
                        "parent_uri_id": collector_id, "json_root": "sources",
                        "results_format": "json"}
        self.runInternalNamedQueryCommand(api_call="get_resources",
                                          params= merge_dicts(slave=params, master=local_params),
                                          callback=callback)
    def getPartitions(self, callback, params=None):
        local_params = {"uri_name": "partitions",
                          "results_format": "json", "json_root": "data"}

        self.runInternalNamedQueryCommand(api_call="get_resources",
                                          params=local_params,
                                          callback=callback)

    def getUsers(self, callback, params=None):
        local_params = {"uri_name": "users",
                          "results_format": "json", "json_root": "data"}

        self.runInternalNamedQueryCommand(api_call="get_resources",
                                          params= merge_dicts(slave=params, master=local_params),
                                          callback=callback)

    def getRoles(self, callback, params=None):
        local_params = {"uri_name": "roles",
                          "results_format": "json", "json_root": "data"}

        self.runInternalNamedQueryCommand(api_call="get_resources",
                                          params=local_params,
                                          callback=callback)

    def getScheduledViews(self, callback, params=None):
        local_params = {"uri_name": "scheduledViews",
                          "results_format": "json", "json_root": "data"}

        self.runInternalNamedQueryCommand(api_call="get_resources",
                                          params=local_params,
                                          callback=callback)

    def getFolder(self, callback, params=None):
        local_params = {"api_version": "v2", "uri_name": "content/folders", "uri_id":  params['request_params']['folder_type'], "results_format": "json", "json_root": None}

        self.runInternalNamedQueryCommand(api_call="get_resources",
                                          params=merge_dicts(master=params, slave=local_params),
                                          callback=callback)

    def getContentExportJob(self, callback, params=None):
        local_params = {"method": "get", "api_version": "v2", "parent_uri_name": "content", "results_format": "json", "json_root": None}

        self.runInternalNamedQueryCommand(api_call="get_resources",
                                          params=merge_dicts(master=params, slave=local_params),
                                          callback=callback)

    def startContentExportJob(self, callback, params=None):
        local_params = {"method": "post", "api_version": "v2", "parent_uri_name": "content","results_format": "json", "json_root": None}

        self.runInternalNamedQueryCommand(api_call="get_resources",
                                          params=merge_dicts(master=params, slave=local_params),
                                          callback=callback)


    def getFERs(self, callback, params=None):
        local_params = {"uri_name": "extractionRules",
                          "results_format": "json", "json_root": "data"}

        self.runInternalNamedQueryCommand(api_call="get_resources",
                                          params=local_params,
                                          callback=callback)

    def getRestValues(self, api_call=None, params=None, callback=None):

        self.Command.createAndRun(callback=callback, sumo=self.sumo,
                                  api_call=api_call, params=params,
                                  encoding=self.encoding,
                                  options={'show_query': self.show_query},
                                  timeout=self.timeout, silenceErrors=False)

    def execute(self, callback,
                params=None, stream=None):
        local_params = {"uri_name": "search/jobs", "json_root": None,
                        "method": "post", "results_format": "json"}

        self.Command.createAndRun(callback=callback, sumo=self.sumo,
                                  api_call="get_resources",
                                  params=merge_dicts(master=params, slave=local_params), encoding=self.encoding,
                                  options={'show_query': self.show_query},
                                  timeout=self.timeout, silenceErrors=False)

    def search_job_polling(self, callback, params=None):
        local_params = {"uri_name": "search/jobs",
                        "results_format": "json", "json_root": None,
                        "method": "get", "request_params": None}

        self.Command.createAndRun(callback=callback, sumo=self.sumo,
                                  api_call="get_resources",
                                  params=merge_dicts(master=params, slave=local_params),
                                  encoding=self.encoding,
                                  options={'show_query': self.show_query},
                                  timeout=self.timeout, silenceErrors=False)

    def get_job_messages(self, callback, job_id, params=None):
        local_params = {"parent_uri_name": "search/jobs",
                        "parent_uri_id": job_id, "uri_name": "messages",
                        "results_format": "grid", "json_root": "messages",
                        "method": "get",  "request_params": None}

        self.Command.createAndRun(callback=callback, sumo=self.sumo,
                                  api_call="get_resources",
                                  params=merge_dicts(master=params, slave=local_params),
                                  encoding=self.encoding,
                                  options={'show_query': self.show_query},
                                  timeout=self.timeout, silenceErrors=False)

    def get_job_records(self, callback, job_id, params=None):
        local_params = {"parent_uri_name": "search/jobs",
                        "parent_uri_id": job_id, "uri_name": "records",
                        "results_format": "grid", "json_root": "records",
                        "method": "get",  "request_params": None}

        self.Command.createAndRun(callback=callback, sumo=self.sumo,
                                  api_call="get_resources",
                                  params=merge_dicts(master=params, slave=local_params),
                                  encoding=self.encoding,
                                  options={'show_query': self.show_query},
                                  timeout=self.timeout, silenceErrors=False)

    @staticmethod
    def setTimeout(timeout):
        Connection.timeout = timeout
        logger.info('Connection timeout set to {0} seconds'.format(timeout))

    @staticmethod
    def setHistoryManager(manager):
        Connection.history = manager
        size = manager.getMaxSize()
        logger.info('Connection history size is {0}'.format(size))
