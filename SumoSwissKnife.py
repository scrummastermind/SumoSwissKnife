__version__ = "v0.0.1"

import sys
import os
import logging
from collections import OrderedDict
import time
import json
import re
import sublime
import webbrowser
import string
import copy


from sublime_plugin import WindowCommand, EventListener, TextCommand
from Default.paragraph import expand_to_paragraph
from .SumoSwissKnifeAPI.Storage import Storage, Settings
from .SumoSwissKnifeAPI.sumologic import SumoAPIException
from .SumoSwissKnifeAPI.Connection import Connection
from .SumoSwissKnifeAPI.History import History
from .SumoSwissKnifeAPI.Completion import Completion
from .SumoSwissKnifeAPI.Utils import get_time_window_mappings_list,\
    get_query_time_window, printProgressBar, get_all_timezones,\
    get_tz_specifc_time, get_tz_specifc_ts, parseRawJson, saveRawJson, get_formatted_results, toTitle, pprint


MESSAGE_RUNNING_CMD = 'Calling Sumo Logic Endpoint...'
SYNTAX_PLAIN_TEXT = 'Packages/Text/Plain text.tmLanguage'
SYNTAX_Sumo = 'Packages/SumoSwissKnife/SumoLogic.sublime-syntax'
COMPLETIONS_Sumo = 'SumoLogic_completions.json'
SumoSwissKnife_SETTINGS_FILE = 'SumoSwissKnife.sublime-settings'
SumoSwissKnife_CONNECTIONS_FILE = 'SumoSwissKnifeConnections.sublime-settings'
SumoSwissKnife_QUERIES_FILE = 'SumoSwissKnifeSavedQueries.sublime-settings'
SumoSwissKnife_METADATA_FOLDER = 'SumoSwissKnife_DB'


USER_FOLDER = None


DEFAULT_FOLDER = None
SETTINGS_FILENAME = None
SETTINGS_FILENAME_DEFAULT = None
CONNECTIONS_FILENAME = None
CONNECTIONS_FILENAME_DEFAULT = None
QUERIES_FILENAME = None
QUERIES_FILENAME_DEFAULT = None
settingsStore = None
queriesStore = None
connectionsStore = None
historyStore = None
SUMOLOGIC_COMPLETIONS = None

DEFAULT_LOG_LEVEL = logging.WARNING
plugin_logger = logging.getLogger(__package__)

plugin_logger.propagate = False
if not plugin_logger.handlers:
    plugin_logger_handler = logging.StreamHandler()
    plugin_logger_formatter = logging.Formatter(
        "[{name}] {levelname}: {message}", style='{')
    plugin_logger_handler.setFormatter(plugin_logger_formatter)
    plugin_logger.addHandler(plugin_logger_handler)
plugin_logger.setLevel(DEFAULT_LOG_LEVEL)
logger = logging.getLogger(__name__)


def getSublimeUserFolder():
    return os.path.join(sublime.packages_path(), 'User')


def startPlugin():
    global USER_FOLDER, DEFAULT_FOLDER
    global SETTINGS_FILENAME, SETTINGS_FILENAME_DEFAULT
    global CONNECTIONS_FILENAME, CONNECTIONS_FILENAME_DEFAULT
    global QUERIES_FILENAME, QUERIES_FILENAME_DEFAULT, METADATA_FOLDER
    global settingsStore, queriesStore, connectionsStore, historyStore
    global decoder, SUMOLOGIC_COMPLETIONS

    USER_FOLDER = getSublimeUserFolder()
    DEFAULT_FOLDER = os.path.dirname(__file__)

    SETTINGS_FILENAME = os.path.join(USER_FOLDER, SumoSwissKnife_SETTINGS_FILE)
    COMPLETIONS_FILENAME = os.path.join(DEFAULT_FOLDER, COMPLETIONS_Sumo)

    SETTINGS_FILENAME_DEFAULT = os.path.join(
        DEFAULT_FOLDER, SumoSwissKnife_SETTINGS_FILE)

    CONNECTIONS_FILENAME = os.path.join(
        USER_FOLDER, SumoSwissKnife_CONNECTIONS_FILE)

    CONNECTIONS_FILENAME_DEFAULT = os.path.join(
        DEFAULT_FOLDER, SumoSwissKnife_CONNECTIONS_FILE)

    QUERIES_FILENAME = os.path.join(USER_FOLDER, SumoSwissKnife_QUERIES_FILE)

    METADATA_FOLDER = os.path.join(USER_FOLDER, SumoSwissKnife_METADATA_FOLDER)

    QUERIES_FILENAME_DEFAULT = os.path.join(
        DEFAULT_FOLDER, SumoSwissKnife_QUERIES_FILE)

    decoder = json.JSONDecoder()

    if not os.path.exists(METADATA_FOLDER):
        os.mkdir(METADATA_FOLDER)

    try:
        settingsStore = Settings(SETTINGS_FILENAME,
                                 default=SETTINGS_FILENAME_DEFAULT)

        SUMOLOGIC_COMPLETIONS = {}

        if COMPLETIONS_FILENAME:
            with open(COMPLETIONS_FILENAME, 'r') as f:
                SUMOLOGIC_COMPLETIONS = json.load(f)

    except Exception:
        msg = '{0}: Failed to parse {1} file'.format(
            __package__, SumoSwissKnife_SETTINGS_FILE)
        logging.exception(msg)
        Window().status_message(msg)

    try:
        connectionsStore = Settings(CONNECTIONS_FILENAME,
                                    default=CONNECTIONS_FILENAME_DEFAULT)
    except Exception:
        msg = '{0}: Failed to parse {1} file'.format(
            __package__, SumoSwissKnife_CONNECTIONS_FILE)
        logging.exception(msg)
        Window().status_message(msg)

    queriesStore = Storage(QUERIES_FILENAME, default=QUERIES_FILENAME_DEFAULT)
    historyStore = History(settingsStore.get('history_size', 100))

    if settingsStore.get('debug', False):
        plugin_logger.setLevel(logging.DEBUG)
    else:
        plugin_logger.setLevel(DEFAULT_LOG_LEVEL)

    Connection.setTimeout(settingsStore.get('thread_timeout', 15))
    Connection.setHistoryManager(historyStore)

    logger.info('plugin (re)loaded')
    logger.info('version %s', __version__)


def readConnections():
    mergedConnections = {}

    if not connectionsStore:
        startPlugin()

    globalConnectionsDict = connectionsStore.get('connections', {})
    projectConnectionsDict = {}
    projectData = Window().project_data()

    if projectData:
        projectConnectionsDict = projectData.get('connections', {})

    mergedConnections = globalConnectionsDict.copy()
    mergedConnections.update(projectConnectionsDict)

    ordered = OrderedDict(sorted(mergedConnections.items()))

    return ordered


def getDefaultConnectionName():
    default = connectionsStore.get('default', False)
    if not default:
        return
    return default


def createOutput(panel=None, name=None, syntax=None,
                 prependText=None, show_result_on_window_rt=True, read_only = True):
    onInitialOutput = None
    if not panel:
        panel, onInitialOutput = getOutputPlace(
            syntax=syntax, name=name,
            show_result_on_window_rt=show_result_on_window_rt) if name else \
            getOutputPlace(
            syntax, show_result_on_window_rt=show_result_on_window_rt)
    if prependText:
        panel.run_command('append', {'characters': str(prependText)})

    initial = True

    def append(outputContent, params=None):
        nonlocal initial
        if initial:
            initial = False
            if onInitialOutput:
                onInitialOutput()

        panel.set_syntax_file(SYNTAX_Sumo)
        panel.set_read_only(False)
        panel.run_command('append', {'characters': outputContent})
        panel.set_read_only(read_only)

    return append


def toNewTab(content, name="", suffix="SumoSwissKnife Saved Query"):
    resultContainer = Window().new_file()
    resultContainer.set_name(
        ((name + " - ") if name != "" else "") + suffix)
    resultContainer.set_syntax_file(SYNTAX_Sumo)
    resultContainer.run_command('append', {'characters': content})


def insertContent(content):
    view = View()
    viewSettings = view.settings()
    autoIndent = viewSettings.get('auto_indent', True)
    viewSettings.set('auto_indent', False)
    view.run_command('insert', {'characters': content})
    viewSettings.set('auto_indent', autoIndent)


def getOutputPlace(syntax=None,
                   name="SumoSwissKnife Result",
                   show_result_on_window_rt=False):
    showResultOnWindow = settingsStore.get('show_result_on_window', False)
    showResultOnWindow = showResultOnWindow or show_result_on_window_rt
    if not showResultOnWindow:
        resultContainer = Window().find_output_panel(name)
        if resultContainer is None:
            resultContainer = Window().create_output_panel(name)
    else:
        resultContainer = None
        views = Window().views()
        for view in views:
            if view.name() == name:
                resultContainer = view
                break
        if not resultContainer:
            resultContainer = Window().new_file()
            resultContainer.set_name(name)

    resultContainer.set_scratch(True)
    resultContainer.set_read_only(True)
    resultContainer.settings().set("word_wrap", "false")
    resultContainer.set_syntax_file(SYNTAX_Sumo)

    def onInitialOutputCallback():
        if settingsStore.get('clear_output', False):
            resultContainer.set_read_only(False)
            resultContainer.run_command('select_all')
            resultContainer.run_command('left_delete')
            resultContainer.set_read_only(True)

        Window().status_message('')

        if not showResultOnWindow:
            Window().run_command("show_panel", {"panel": "output." + name})

        if settingsStore.get('focus_on_result', False):
            Window().focus_view(resultContainer)

    Window().set_view_index(resultContainer, 1, 0)

    return resultContainer, onInitialOutputCallback


def getSelectionText():
    text = []

    selectionRegions = getSelectionRegions()

    if not selectionRegions:
        return text

    for region in selectionRegions:
        text.append(View().substr(region))

    return text


def getSelectionRegions():
    expandedRegions = []

    if not View().sel():
        return None

    expandTo = settingsStore.get('expand_to', 'file')
    if not expandTo:
        expandTo = 'file'

    expandToParagraph = settingsStore.get('expand_to_paragraph')
    if expandToParagraph is True:
        expandTo = 'paragraph'

    expandTo = str(expandTo).strip()
    if expandTo not in ['file', 'view', 'paragraph', 'line']:
        expandTo = 'file'

    for region in View().sel():
        if region.empty():
            if expandTo in ['file', 'view']:
                region = sublime.Region(0, View().size())
                return [region]
            elif expandTo == 'paragraph':
                region = expand_to_paragraph(View(), region.b)
            else:
                region = View().line(region)

        if not region.empty():
            expandedRegions.append(region)

    return expandedRegions


def getCurrentSyntax():
    view = View()
    currentSyntax = None
    if view:
        currentSyntax = view.settings().get('syntax')
    return currentSyntax


class ST(EventListener):
    selection_counter = {}
    connectionDict = None
    conn = None
    results_pages = None
    time_zone = None
    from_time = None
    to_time = None
    collectors = {}
    savedQueries = {}
    fers = []
    roles = {}
    users = {}
    partitions = []
    views = []
    source_categories = []
    collectors_names = []
    sources_names = []
    sources = []
    foldersProcessed = []
    contentId = None
    contentExportJobId = None
    completion = None
    ready = False
    selected_accessId = None
    current_connection_metadata_folder = None
    results_format = None
    search_job_id = None
    message_count = 0
    record_count = 0
    results_page_size = 0
    indexes = []
    views = []
    last_function_name = None
    last_found = False
    cache = None
    menu_links = {}
    roles = []
    users = []
    role_lookup = {}

    @staticmethod
    def on_selection_modified(view):
        if not view.match_selector(
                view.sel()[0].begin(), 'source.sumo'):
            return

        selected_txt = view.substr(view.sel()[0])
        ST.selection_counter[selected_txt] = \
            ST.selection_counter.get(selected_txt, 0) + 1
        if ST.selection_counter[selected_txt] < 2:
            return

        if not ST.cache:
            path_db = os.path.join(DEFAULT_FOLDER, 'SLQL_Docs_DB.json')

            if os.path.exists(path_db):
                ST.cache = json.load(open(path_db))

        completions = ST.cache

        if completions:
            completion = completions.get(selected_txt)
            found = completion

        if found:
            view.set_status('hint', found['name'] + " | ")
            menus = []
            menus.append(found['name'])
            menus.append('-------------------------------')

            if found['descr'] and found['descr'] != '--':
                menus.append('Description:')
                for descr in re.sub("(.{80,100}[\.]) ", "\\1||",
                                    found["descr"]).split("||"):
                    menus.append("  " + descr)

            if found["params"]:
                menus.append("Parameters:")
            for parameter in found["params"]:
                menus.append(
                    "    - " + parameter["name"] + ": " + parameter["descr"])

            if found['syntax'] and found['syntax'] != '--':
                syntax = found['syntax']
                menus.append('Syntax:')
                syntax_lines = syntax.split('|')
                for idx, syntax_line in enumerate(syntax_lines):
                    if idx > 0:
                        menus.append('  |' + syntax_line)
                    else:
                        first_line = '  *' \
                            if (not syntax_line or syntax_line.isspace()) \
                            else ('  ' + syntax_line)
                        menus.append(first_line)
                menus.append('  ')

            ST.last_found = found

            menu = ST.appendLinks(menus, found)

            view.show_popup_menu(menu, ST.action)
        else:
            view.erase_status('hint')
        ST.selection_counter = {}

    @staticmethod
    def appendLinks(menus, found):
        ST.menu_links = {}
        ST.menu_links[len(menus)] = found['path']

        menus.append(" > Go To: Sumo Docs")
        return menus

    @staticmethod
    def action(item):
        if item in ST.menu_links:
            webbrowser.open_new_tab(ST.menu_links[item])

    @staticmethod
    def bootstrap():
        ST.connectionDict = readConnections()
        ST.setDefaultConnection()

    @staticmethod
    def setDefaultConnection():
        default = getDefaultConnectionName()
        if not default:
            return
        if default not in ST.connectionDict:
            logger.error(
                'connection "%s" set as default, but it does not exists',
                default)
            return
        logger.info('default connection is set to "%s"', default)
        ST.setConnection(default)

    @staticmethod
    def setConnection(connectionName, callback=None):
        if not connectionName:
            return

        if connectionName not in ST.connectionDict:
            return

        settings = settingsStore.all()
        config = ST.connectionDict.get(connectionName)

        promptKeys = [key for key, value in config.items() if value is None]
        promptDict = {}
        logger.info('[setConnection] prompt keys {}'.format(promptKeys))
        ST.selected_accessId = config['accessId']
        ST.current_connection_metadata_folder = os.path.join(
                                     METADATA_FOLDER, ST.selected_accessId)

        if not os.path.exists(METADATA_FOLDER):
            os.mkdir(METADATA_FOLDER)

        if not os.path.exists(ST.current_connection_metadata_folder):
            os.mkdir(ST.current_connection_metadata_folder)

        try:
            os.mkdir(ST.current_connection_metadata_folder)
        except FileExistsError as e:
            logger.info('File already exists')

        def mergeConfig(config, promptedKeys=None):
            merged = config.copy()
            if promptedKeys:
                merged.update(promptedKeys)
            return merged

        def createConnection(connectionName, config, settings, callback=None):
            try:
                ST.conn = Connection(connectionName, config, settings=settings)
            except FileNotFoundError as e:
                Window().status_message(
                    __package__ + ": " + str(e).splitlines()[0])
                raise e
            ST.loadConnectionData(callback)

        if not promptKeys:
            createConnection(connectionName, config, settings, callback)
            return

        def setMissingKey(key, value):
            nonlocal promptDict
            if value is None:
                return
            promptDict[key] = value
            if promptKeys:
                promptNext()
            else:
                merged = mergeConfig(config, promptDict)
                createConnection(connectionName, merged, settings, callback)

        def promptNext():
            nonlocal promptKeys
            if not promptKeys:
                merged = mergeConfig(config, promptDict)
                createConnection(connectionName, merged, settings, callback)
            key = promptKeys.pop()
            Window().show_input_panel('Connection '
                                      + key, '',
                                      lambda userInput: setMissingKey(
                                          key, userInput), None, None)

        promptNext()

    @staticmethod
    def loadConnectionData(callback=None):
        reshape()
        if not View().match_selector(0, 'source.sumo'):
            return None

        update_connection_loading_wip = createOutput(name='Logs',
            syntax=SYNTAX_Sumo, show_result_on_window_rt=False)
        objectsLoaded = 0

        if not ST.conn:
            return

        def afterAllDataHasLoaded(saveRawJsonMeta=False):

            if saveRawJsonMeta:
                saveRawJson(ST.collectors, os.path.join(
                        ST.current_connection_metadata_folder, 'collectors'))

                update_connection_loading_wip('\n - {num} Collectors cached!\n'.format(num=len(ST.collectors)))

                saveRawJson(ST.savedQueries, os.path.join(
                        ST.current_connection_metadata_folder, 'queries'))

                update_connection_loading_wip('\n - {num} Personal Folder queries cached!\n'.format(num=len(ST.savedQueries)))

                saveRawJson(ST.fers, os.path.join(
                        ST.current_connection_metadata_folder, 'fers'))

                update_connection_loading_wip('\n - {num} FERs cached!\n'.format(num=len(ST.fers)))

                saveRawJson(ST.partitions, os.path.join(
                        ST.current_connection_metadata_folder, 'partitions'))

                update_connection_loading_wip('\n - {num} Partitions Indecies cached!\n'.format(num=len(ST.partitions)))

                saveRawJson(ST.views, os.path.join(
                        ST.current_connection_metadata_folder, 'views'))

                update_connection_loading_wip('\n - {num} Scheduled Views cached!\n'.format(num=len(ST.views)))

                saveRawJson(ST.roles, os.path.join(
                        ST.current_connection_metadata_folder, 'roles'))

                update_connection_loading_wip('\n - {num} Roles cached!\n'.format(num=len(ST.roles)))

                saveRawJson(ST.users, os.path.join(
                        ST.current_connection_metadata_folder, 'users'))

                update_connection_loading_wip('\n - {num} Users cached!\n'.format(num=len(ST.users)))

            total_sources = 0

            for collector in ST.collectors.values():

                if 'name' in collector.keys() \
                    and collector['name'] \
                        not in ST.collectors_names:
                    if saveRawJsonMeta:
                        update_connection_loading_wip('\n - Loading The [{collectector_name}] Collector'.format(collectector_name=collector['name']))
                    ST.collectors_names.append(
                        collector['name'])

                if 'category' in collector.keys() \
                    and collector['category'] \
                        not in ST.source_categories:
                    ST.source_categories.append(
                        collector['category'])
                    if saveRawJsonMeta:
                        update_connection_loading_wip('\n --- Category[{sc_name}]'.format(sc_name=collector['category']))
                    total_sources += 1

                if 'sources' in collector.keys():
                    for source in collector['sources']:
                        if 'name' in source.keys() \
                            and source['name'] \
                                not in ST.sources_names:
                            ST.sources_names.append(
                                source['name'])
                            if saveRawJsonMeta:
                                update_connection_loading_wip('\n --- Source[{src_name}]'.format(src_name=source['name']))
                        if 'category' in source.keys() \
                            and source['category'] \
                                not in ST.source_categories:
                            ST.source_categories.append(
                                source['category'])
                            if saveRawJsonMeta:
                                update_connection_loading_wip('\n ------ Category [{sc_name}]'.format(sc_name=source['category']))
                                total_sources += 1

            update_connection_loading_wip('\n - {num} Source Categories Loaded!\n'.format(num=total_sources))

            sv_index_names = [view['indexName'] for view in ST.views]
            part_index_names = [partition['name'] for partition in ST.partitions]

            update_connection_loading_wip('\n - Loading Completions...\n')
            ST.completion = Completion(
                ST.collectors_names, ST.sources_names,
                ST.source_categories, part_index_names,
                sv_index_names, ST.fers, SUMOLOGIC_COMPLETIONS,
                settings=settingsStore)

            ST.ready = True

            update_connection_loading_wip('\n\n - All Completions Loaded...\n')
            update_connection_loading_wip('\n - DONE!\n')

            time.sleep(5)

            Window().run_command("hide_panel", {"panel": "output." + 'Logs'})


        def processCollectors(collectors=None, params=None, saveRawJsonMeta=True):
            items = collectors
            if items and len(items)==1 and 'errors' in items[0].keys():

                error = items[0]

                update_connection_loading_wip('\n{err}\n'.format(err=error['msg']))
                ST.items = items
                nonlocal objectsLoaded
                objectsLoaded += 1

                if objectsLoaded == 7:
                    afterAllDataHasLoaded(saveRawJsonMeta)
                return

            if saveRawJsonMeta:
                ST.collectors = {collector['id']:collector for collector in collectors}
                for id, collector in ST.collectors.items():

                    def sourcesCallback(sources, params=None):
                        try:
                            items = sources
                            if items and len(items)==1 and 'errors' in items[0].keys():
                                error = items[0]
                                update_connection_loading_wip('\n{err}\n'.format(err=error['msg']))
                                ST.items = items
                                return

                            collector['sources'] = sources
                            update_connection_loading_wip(
                                'Adding {num} Sources for Collector [{collector_name}]\n'.
                                format(num=len(sources), collector_name=collector['name']))
                        except Exception as e:
                                update_connection_loading_wip('\n{err}\n'.format(err=str(e)))

                    time.sleep(0.6)

                    ST.conn.getSources(id,
                                       params=params,
                                       callback=sourcesCallback)
            else:
                ST.collectors = None
                ST.collectors = copy.deepcopy(collectors)

            loaded = 'Loaded' if saveRawJsonMeta else 'Loaded From Cache'

            update_connection_loading_wip(' - {num} Collectors Loaded From Cache...\n'.format(Loaded=loaded, num=len(collectors)))

            nonlocal objectsLoaded
            objectsLoaded += 1
            if objectsLoaded == 7:
                afterAllDataHasLoaded(saveRawJsonMeta)

        def collectorsCallback(collectors, params=None):
            processCollectors(collectors=collectors, params=params)

        def processFERs(fers=None, params=None, saveRawJsonMeta=True):
            items = fers
            if items and len(items)==1 and 'errors' in items[0].keys():

                error = items[0]

                update_connection_loading_wip('\n{err}\n'.format(err=error['msg']))
                ST.items = items
                nonlocal objectsLoaded
                objectsLoaded += 1

                if objectsLoaded == 7:
                    afterAllDataHasLoaded(saveRawJsonMeta)
                return

            loaded = 'Loaded' if saveRawJsonMeta else 'Loaded From Cache'

            if ST.fers and len(ST.fers) > 0:
                ST.fers += fers

            else:
                ST.fers = fers

            if params:
                if 'request_params' in params:
                    request_params = params['request_params']
                    if request_params and 'token' in request_params.keys():
                        next_token = request_params['token']
                        time.sleep(0.6)
                        update_connection_loading_wip(' - Getting FERs Views next batch {next}\n'.format(next=next_token))

                        ST.conn.getFERs(callback=usersCallback, params=params)


            nonlocal objectsLoaded
            objectsLoaded += 1

            update_connection_loading_wip(' - {num} FERs {Loaded}...\n'.format(Loaded=loaded, num=len(ST.fers)))
            if objectsLoaded == 7:
                afterAllDataHasLoaded(saveRawJsonMeta)

        def fersCallback(fers, params=None):
            processFERs(fers=fers, params=params)

        def processRoles(roles=None, params=None, saveRawJsonMeta=True):
            items = roles
            if items and len(items)==1 and 'errors' in items[0].keys():

                error = items[0]

                update_connection_loading_wip('\n{err}\n'.format(err=error['msg']))
                ST.items = items
                nonlocal objectsLoaded
                objectsLoaded += 1

                if objectsLoaded == 7:
                    afterAllDataHasLoaded(saveRawJsonMeta)
                return

            loaded = 'Loaded' if saveRawJsonMeta else 'Loaded From Cache'

            if ST.roles and len(ST.roles) > 0:
                ST.roles += roles

            else:
                ST.roles = roles

            for role in roles:
                ST.role_lookup[role['id']]= {'name': role['name'],
                'capabilities': role['capabilities']}

            update_connection_loading_wip(' - {num} Roles {Loaded}...\n'.format(Loaded=loaded, num=len(ST.roles)))

            if params:
                if 'request_params' in params:
                    request_params = params['request_params']
                    if request_params and 'token' in request_params.keys():
                        next_token = request_params['token']
                        time.sleep(0.6)
                        update_connection_loading_wip(' - Getting Roles next batch {next}\n'.format(next=next_token))

                        ST.conn.getRoles(callback=usersCallback, params=params)

            nonlocal objectsLoaded
            objectsLoaded += 1

            if objectsLoaded == 7:
                afterAllDataHasLoaded(saveRawJsonMeta)

        def rolesCallback(roles, params=None):
            processRoles(roles=roles, params=params)

        def processUsers(users=None, params=None, saveRawJsonMeta=True):
            items = users
            if items and len(items)==1 and 'errors' in items[0].keys():

                error = items[0]

                update_connection_loading_wip('\n{err}\n'.format(err=error['msg']))
                ST.items = items
                nonlocal objectsLoaded
                objectsLoaded += 1

                if objectsLoaded == 7:
                    afterAllDataHasLoaded(saveRawJsonMeta)
                return

            loaded = 'Loaded' if saveRawJsonMeta else 'Loaded From Cache'

            if ST.users and len(ST.users) > 0:
                ST.users += users

            else:
                ST.users = users

            update_connection_loading_wip(' - {num} Users {Loaded}...\n'.format(Loaded=loaded, num=len(ST.users)))

            if params:
                if 'request_params' in params:
                    request_params = params['request_params']
                    if request_params and 'token' in request_params.keys():
                        next_token = request_params['token']
                        time.sleep(0.6)
                        update_connection_loading_wip(' - Getting Users next batch {next}\n'.format(next=next_token))

                        ST.conn.getUsers(callback=usersCallback, params=params)

            nonlocal objectsLoaded
            objectsLoaded += 1

            if objectsLoaded == 7:
                afterAllDataHasLoaded(saveRawJsonMeta)

        def usersCallback(users, params=None):
            processUsers(users=users, params=params)

        def contentExportJobStatus(contentExportJobStatusJson, params=None):
            status = contentExportJobStatusJson['status']

            update_connection_loading_wip('\n - Loading {folder} Folder Contents.. {status}.\n'.format(folder=toTitle(ST.folderTypeWIP), status=status))

            if status == 'Success':
                params['uri_id'] = '{uri_id}/result'.format(uri_id=ST.contentExportJobId)
                ST.conn.getContentExportJob(callback=contentExportJobResult,params=params)
            elif status == 'InProgress':
                params['uri_id'] = '{uri_id}/status'.format(uri_id=ST.contentExportJobId)
                time.sleep(1)
                ST.conn.getContentExportJob(callback=contentExportJobStatus, params=params)

        def flatten_json(y):
            out = {}

            def flatten(x, name=''):
                if type(x) is dict:
                    for a in x:
                        flatten(x[a], name + a + '_')
                elif type(x) is list:
                    i = 0
                    for a in x:
                        flatten(a, name + str(i) + '_')
                        i += 1
                else:
                    out[name[:-1]] = x

            flatten(y)
            return out

        def processContentExportJobResult(contentExportJobStatusJson=None, params=None, saveRawJsonMeta=True, cached_queries=None):

            loaded = 'Loaded' if saveRawJsonMeta else 'Loaded From Cache'

            if cached_queries and not saveRawJsonMeta:
               update_connection_loading_wip(' - {num} Personal Folder Contents {Loaded}...\n'.format(Loaded=loaded, num=len(cached_queries)))
               ST.savedQueries = cached_queries
               nonlocal objectsLoaded
               objectsLoaded += 1
               if objectsLoaded == 7:
                afterAllDataHasLoaded(saveRawJsonMeta)
               ST.folderTypeWIP = None
               return

            panels_queries = {}
            saved_queries = {}
            all_queries = {}

            flatten_personal_folder = flatten_json(contentExportJobStatusJson)

            panels_keys = [k.replace('_name', '').replace('_queryString', '') for k in flatten_personal_folder.keys() if 'panels' in k and ( k.endswith('_name') or k.endswith('_queryString'))]
            saved_searches_keys = [k.replace('_search_queryText', '') for k in flatten_personal_folder.keys() if k.endswith('_search_queryText')]

            panels_keys = list(dict.fromkeys(panels_keys))
            saved_searches_keys = list(dict.fromkeys(saved_searches_keys))

            for panels_key in panels_keys:
                try:
                    name_key = "{panels_key}_name".format(panels_key=panels_key)
                    query_key = "{panels_key}_queryString".format(panels_key=panels_key)

                    if query_key not in flatten_personal_folder.keys():
                        continue

                    panel_queryText = flatten_personal_folder.get(query_key, None)
                    panel_name = flatten_personal_folder.get(name_key, None)

                    if not panel_name or not panel_queryText:
                        continue

                    panels_queries[panel_name] = panel_queryText
                except KeyError as e:
                    logger.info(e)

            for saved_searches_key in saved_searches_keys:
                try:
                    name_key = "{saved_searches_key}_name".format(saved_searches_key=saved_searches_key)
                    query_key = "{saved_searches_key}_search_queryText".format(saved_searches_key=saved_searches_key)

                    if query_key not in flatten_personal_folder.keys():
                        continue

                    search_queryText = flatten_personal_folder.get(query_key, None)
                    search_name = flatten_personal_folder.get(name_key, None)

                    if not search_name or not search_queryText:
                        continue

                    saved_queries[search_name] = search_queryText
                except KeyError as e:
                    logger.info(e)

            all_queries.update(panels_queries)
            all_queries.update(saved_queries)

            ST.savedQueries = all_queries

            if ST.folderTypeWIP == 'personal' and 'personal' not \
                in ST.foldersProcessed:
                ST.foldersProcessed.append('personal')
            objectsLoaded += 1
            if objectsLoaded == 7:
                afterAllDataHasLoaded(saveRawJsonMeta)

            ST.folderTypeWIP = None

        def contentExportJobResult(contentExportJobStatusJson, params=None):
            processContentExportJobResult(contentExportJobStatusJson=contentExportJobStatusJson, params=params)

        def getContentExportJobId(contentExportJobJson, params=None):
            ST.contentExportJobId = contentExportJobJson['id']
            params["method"] = "get"
            params['parent_uri_id'] = ST.contentId
            params['uri_name'] = 'export'
            params['uri_id'] = '{uri_id}/status'.format(uri_id=ST.contentExportJobId)

            ST.conn.getContentExportJob(callback=contentExportJobStatus,params=params)

        def onFolderInfoReceivedCallback(folderInfoJson, params=None):
            ST.folderTypeWIP = params['request_params']['folder_type']

            update_connection_loading_wip('\n - Loading {folder} Folder Contents...\n'.format(folder=toTitle(ST.folderTypeWIP)))

            if ST.folderTypeWIP == 'personal':
                ST.contentId = folderInfoJson['id']
                params['parent_uri_id'] = ST.contentId
                params['uri_name'] = 'export'
                params['uri_id'] = None
                ST.conn.startContentExportJob(callback=getContentExportJobId, params=params)
            else:
                raise(Exception('blow'))

        def processPartitions(partitions=None, params=None, saveRawJsonMeta=True):

            loaded = 'Loaded' if saveRawJsonMeta else 'Loaded From Cache'
            ST.partitions = partitions

            update_connection_loading_wip(' - {num} Partitions Indecies {Loaded}...\n'.format(Loaded=loaded, num=len(ST.partitions)))

            nonlocal objectsLoaded
            objectsLoaded += 1
            if objectsLoaded == 7:
                afterAllDataHasLoaded(saveRawJsonMeta)

        def partitionsCallback(partitions, params=None):
            processPartitions(partitions=partitions, params=params)

        def processScheduledViews(views=None, params=None, saveRawJsonMeta=True):
            loaded = 'Loaded' if saveRawJsonMeta else 'Loaded From Cache'

            if ST.views and len(ST.views) > 0:
                ST.views += views

            else:
                ST.views = views

            if params:
                if 'request_params' in params:
                    request_params = params['request_params']
                    if request_params and 'token' in request_params.keys():
                        next_token = request_params['token']
                        time.sleep(0.6)
                        update_connection_loading_wip(' - Getting Scheduled Views next batch {next}\n'.format(next=next_token))

                        ST.conn.getScheduledViews(callback=usersCallback, params=params)

            nonlocal objectsLoaded
            objectsLoaded += 1
            update_connection_loading_wip(' - {num} Scheduled Views {Loaded}...\n'.format(Loaded=loaded, num=len(ST.views)))
            if objectsLoaded == 7:
                afterAllDataHasLoaded(saveRawJsonMeta)

        def scheduledViewsCallback(views, params=None):
            processScheduledViews(views=views, params=params)

        current_connection_metadata_collectors =\
            parseRawJson(os.path.join(
                                  ST.current_connection_metadata_folder,
                                  'collectors'))

        current_connection_metadata_queries =\
            parseRawJson(os.path.join(
                                  ST.current_connection_metadata_folder,
                                  'queries'))

        current_connection_metadata_fers =\
            parseRawJson(os.path.join(
                                  ST.current_connection_metadata_folder,
                                  'fers'))

        current_connection_metadata_partitions =\
            parseRawJson(os.path.join(
                                  ST.current_connection_metadata_folder,
                                  'partitions'))

        current_connection_metadata_views =\
            parseRawJson(os.path.join(
                                  ST.current_connection_metadata_folder,
                                  'views'))

        current_connection_metadata_users =\
            parseRawJson(os.path.join(
                                  ST.current_connection_metadata_folder,
                                  'users'))

        current_connection_metadata_roles =\
            parseRawJson(os.path.join(
                                  ST.current_connection_metadata_folder,
                                  'roles'))

        if current_connection_metadata_collectors:
            processCollectors(
                collectors=current_connection_metadata_collectors,
                saveRawJsonMeta=False, params=None)

        else:
            ST.conn.getCollectors(callback=collectorsCallback, params={"request_params": {"limit": 1000}})

        if current_connection_metadata_queries:
            processContentExportJobResult(cached_queries=current_connection_metadata_queries, params=None,saveRawJsonMeta=False)

        else:
            ST.conn.getFolder(
                callback=onFolderInfoReceivedCallback,
                params={"request_params":{'folder_type': 'personal'}})

        if current_connection_metadata_fers:
            processFERs(fers=current_connection_metadata_fers, params=None, saveRawJsonMeta=False)

        else:
            ST.conn.getFERs(callback=fersCallback, params=None)

        if current_connection_metadata_partitions:
            processPartitions(partitions=current_connection_metadata_partitions, params=None, saveRawJsonMeta=False)
        else:
            ST.conn.getPartitions(callback=partitionsCallback, params=None)

        if current_connection_metadata_views:
            processScheduledViews(views=current_connection_metadata_views, params=None, saveRawJsonMeta=False)

        else:
            ST.conn.getScheduledViews(
                callback=scheduledViewsCallback, params=None)

        if current_connection_metadata_roles:
            processRoles(roles=current_connection_metadata_roles, params=None, saveRawJsonMeta=False)

        else:
            ST.conn.getRoles(
                callback=rolesCallback, params={"request_params": {"sortBy": "name"}})

        if current_connection_metadata_users:
            processUsers(users=current_connection_metadata_users, params=None, saveRawJsonMeta=False)

        else:
            ST.conn.getUsers(
                callback=usersCallback, params={"request_params": {"sortBy": "firstName"}})

    @staticmethod
    def showFERParseExpression(callback=None):
        menu = [fer['name'] for fer in ST.fers]

        def onFERSelected(index):
            selected_fer = ST.fers[index]
            show_fer_expression = createOutput(
                name=selected_fer['name'], read_only=False)

            query = selected_fer['scope'] + '|' + \
                selected_fer['parseExpression']
            syntax_lines = query.split('|')

            for idx, syntax_line in enumerate(syntax_lines):
                if idx > 0:
                    show_fer_expression('|' + syntax_line)
                else:
                    first_line = '*' if (not syntax_line or syntax_line.isspace()) else syntax_line
                    show_fer_expression(first_line)

        Window().show_quick_panel(menu, lambda index:
                                  onFERSelected(index))

    @staticmethod
    def showPanelQuery(callback=None):
        menu = list(ST.savedQueries.keys())

        def onPanelSelected(index):
            selected_panel_name = menu[index]
            selected_panel = ST.savedQueries[selected_panel_name]
            show_panel_query = createOutput(name=selected_panel_name, read_only=False)
            show_panel_query(selected_panel)

        Window().show_quick_panel(menu, lambda index:
                                  onPanelSelected(index))

    @staticmethod
    def beautifyField(edit):
        selected_region = View().sel()[0]
        selected_field_word_region = View().word(selected_region)
        selected_text = View().substr(selected_field_word_region)
        selected_fields = selected_text.split(',')
        selected_fields = [field.strip() for field in selected_fields]

        for selected_field in selected_fields:
            phrase = selected_field.replace('_', '', 1)
            phrase = selected_field.replace('_', ' ').strip()
            phrase = "%\"{phrase}\"".format(phrase=phrase)
            phrase = toTitle(phrase)
            View().insert(edit,
                View().line(selected_field_word_region).end()
                 , "\n| {select_field} as {phrase}\n| fields - {select_field}".format(select_field=selected_field, phrase=phrase))

    @staticmethod
    def showAllCollectors(callback=None):
        print(ST.collectors)
        if not ST.results_format:
            ST.selectResultsFormatQuickPanel(
                 callback=lambda: Window().run_command(
                                          'st_show_all_collectors'))

        collectors = []
        show_results = createOutput(
                                     name="{0}_{1}".format
                                     (
                                         ST.conn.accessId, ' - All_Collectors'
                                     ),
                                     syntax=SYNTAX_Sumo)

        for collector in ST.collectors.values():
            if 'sources' in collector.keys():
                collector.pop('sources', None)
            collectors.append(collector)

        collectors = list(ST.collectors.values())
        show_results(get_formatted_results(
            results_format=ST.results_format,json_raw_data=collectors))


    @staticmethod
    def showAllUsers(callback=None):
        if not ST.results_format:
            ST.selectResultsFormatQuickPanel(
                 callback=lambda: Window().run_command(
                                          'st_show_all_users'))
        show_results = createOutput(
                                     name="{0}_{1}".format
                                     (
                                         ST.conn.accessId, ' - All_Users'
                                     ),
                                     syntax=SYNTAX_Sumo)
        local_users = ST.users
        if ST.role_lookup and len(ST.role_lookup.keys()) > 0:
            local_users = []
            for user in ST.users:
                roles = []
                for roleId in user['roleIds']:
                    if roleId in ST.role_lookup.keys():
                        role = ST.role_lookup[roleId]
                        roles.append(toTitle(role['name']))
                        user['roles'] = roles
                local_users.append(user)

        show_results(get_formatted_results(results_format=ST.results_format,json_raw_data=local_users))

    @staticmethod
    def showAllRoles(callback=None):
        if not ST.results_format:
            ST.selectResultsFormatQuickPanel(
                 callback=lambda: Window().run_command(
                                          'st_show_all_roles'))
        show_results = createOutput(
                                     name="{0}_{1}".format
                                     (
                                         ST.conn.accessId, ' - All_Roles'
                                     ),
                                     syntax=SYNTAX_Sumo)
        vals = ST.roles
        show_results(get_formatted_results(results_format=ST.results_format,json_raw_data=vals))

    @staticmethod
    def showAllScheduledViews(callback=None):
        if not ST.results_format:
            ST.selectResultsFormatQuickPanel(
                 callback=lambda: Window().run_command(
                                          'st_show_all_schedueled_views'))
        show_results = createOutput(
                                     name="{0}_{1}".format
                                     (
                                         ST.conn.accessId, ' - All_Scheduled_Views'
                                     ),
                                     syntax=SYNTAX_Sumo)
        vals = ST.views
        show_results(get_formatted_results(results_format=ST.results_format,json_raw_data=vals))

    @staticmethod
    def showAllPartitions(callback=None):
        if not ST.results_format:
            ST.selectResultsFormatQuickPanel(
                 callback=lambda: Window().run_command(
                                          'st_show_all_partitions'))
        show_results = createOutput(
                                     name="{0}_{1}".format
                                     (
                                         ST.conn.accessId, ' - All_Partitions'
                                     ),
                                     syntax=SYNTAX_Sumo)
        vals = ST.partitions
        show_results(get_formatted_results(results_format=ST.results_format,json_raw_data=vals))

    @staticmethod
    def showAllFers(callback=None):
        if not ST.results_format:
            ST.selectResultsFormatQuickPanel(
                 callback=lambda: Window().run_command(
                                          'st_show_all_fers'))
        show_results = createOutput(
                                     name="{0}_{1}".format
                                     (
                                         ST.conn.accessId, ' - All_Fers'
                                     ),
                                     syntax=SYNTAX_Sumo)
        vals = ST.fers
        show_results(get_formatted_results(results_format=ST.results_format,json_raw_data=vals))

    @staticmethod
    def showAllSources(callback=None):
        if not ST.results_format:
            ST.selectResultsFormatQuickPanel(
                 callback=lambda: Window().run_command(
                                          'st_show_all_sources'))
        show_results = createOutput(
                                     name="{0}_{1}".format
                                     (
                                         ST.conn.accessId, ' - All_Sources'
                                     ),
                                     syntax=SYNTAX_Sumo)
        sources = []

        for collector in ST.collectors.values():
            if 'sources' in collector.keys():
                sources.append(collector['sources'])


        flatten = lambda l: [item for sublist in l for item in sublist]
        flattened_sources = flatten(sources)

        show_results(get_formatted_results(
            results_format=ST.results_format, json_raw_data=flattened_sources))

    @staticmethod
    def showScheduledViewQuery(callback=None):
        menu = [view['indexName'] for view in ST.views]

        def onSVSelected(index):
            selected_sv = ST.views[index]
            show_sv_query = createOutput(name=selected_sv['indexName'], read_only=False)

            syntax_lines = selected_sv['query'].split('|')

            for idx, syntax_line in enumerate(syntax_lines):
                if idx > 0:
                    show_sv_query('\n|' + syntax_line)
                else:
                    first_line = '*' if (not syntax_line or syntax_line.isspace()) else syntax_line
                    show_sv_query(first_line)

        Window().show_quick_panel(menu, lambda index:
                                  onSVSelected(index))

    @staticmethod
    def selectConnectionQuickPanel(callback=None):
        ST.connectionDict = readConnections()
        if len(ST.connectionDict) == 0:
            sublime.message_dialog('You need to setup your connections first.')
            return

        def connectionMenuList(connDictionary):
            menuItemsList = []
            template = '{accessId}'
            for name, config in ST.connectionDict.items():
                accessId = config.get('accessId', None)
                connectionInfo = template.format(
                    accessId=accessId)
                menuItemsList.append([name, connectionInfo])
                menuItemsList.sort()
            return menuItemsList

        def onConnectionSelected(index, callback):
            ST.results_pages = None
            ST.time_zone = settingsStore.get('time_zone', 'utc')
            ST.results_format = settingsStore.get('results_format', 'grid')
            ST.from_time = None
            ST.to_time = None
            ST.collectors = {}
            ST.savedQueries = {}
            ST.fers = []
            ST.roles = {}
            ST.users = {}
            ST.partitions = []
            ST.views = []
            ST.source_categories = []
            ST.collectors_names = []
            ST.sources_names = []
            ST.sources = []
            ST.foldersProcessed = []
            ST.contentId = None
            ST.contentExportJobId = None
            ST.completion = None
            ST.ready = False
            ST.selected_accessId = None
            ST.current_connection_metadata_folder = None
            ST.search_job_id = None
            ST.message_count = 0
            ST.record_count = 0
            ST.results_page_size = settingsStore.get('results_page_size', 250)
            ST.indexes = []
            ST.views = []
            ST.last_function_name = None
            ST.last_found = False
            ST.cache = None
            ST.menu_links = {}
            ST.roles = []
            ST.users = []
            ST.role_lookup = {}

            menuItemsList = connectionMenuList(ST.connectionDict)
            if index < 0 or index >= len(menuItemsList):
                return
            connectionName = menuItemsList[index][0]
            ST.setConnection(connectionName, callback)
            logger.info('Connection "{0}" selected'.format(connectionName))
            if callback:
                callback()

        menu = connectionMenuList(ST.connectionDict)
        # show pannel with callback above
        Window().show_quick_panel(menu, lambda index:
                                  onConnectionSelected(index, callback))

    @staticmethod
    def selectResultsFormatQuickPanel(callback=None):
        formats = {'Plain': 'plain', 'Simple': 'simple', 'Github': 'github',
                            'Grid': 'grid', 'Fancy Grid': 'fancy_grid',
                            'Pipe': 'pipe', 'Orgtbl': 'orgtbl', 'Jira': 'jira',
                            'Presto': 'presto', 'P Sql': 'psql', 'Rst': 'rst',
                            'Media Wiki': 'mediawiki', 'Moinmoin': 'moinmoin',
                            'Youtrack': 'youtrack', 'HTML': 'html',
                            'Latex': 'latex', 'Latex Raw': 'latex_raw',
                            'Latex Booktabs': 'latex_booktabs',
                            'Textile': 'textile', 'Json': 'json_pretty',
                            'CSV': 'csv'}
        menu = [str(key) for key in formats.keys()]

        def onFormatSelected(index, callback):
            format_key = menu[index]
            ST.results_format = formats[format_key]
            callback()

        Window().show_quick_panel(menu, lambda index:
                                  onFormatSelected(index, callback))

    @staticmethod
    def selectTimeZoneQuickPanel():
        time_zones = get_all_timezones()

        def onTimeZoneSelected(index):
            ST.time_zone = time_zones[index]
            ST.selectTimeWindowQuickPanel(callback=lambda
                                          fromTime, toTime:
                                          Window().run_command(
                                            'st_execute_all', {
                                                'fromTime': fromTime,
                                                'toTime': toTime}))

        Window().show_quick_panel(get_all_timezones(), lambda index:
                                  onTimeZoneSelected(index))

    @staticmethod
    def selectResultsMessagesPageQuickPanel():
        page_size = 250
        message_count = ST.message_count
        results_pages = OrderedDict()
        total = message_count
        left_over = 0
        pages = 0

        if total > 0:
            left_over = message_count % page_size
            deviadable = total - left_over
            deviadable = deviadable if deviadable > 0 else 0
            pages = int(deviadable / page_size)

        for page in range (1, pages+1):
            end = page * page_size
            start = end - (page_size - 1)
            key = "Page {page}, Messages [{start} - {end}]".format(page=page, start=start, end=end)
            results_pages[key] = {'offset': start-1, 'limit': page_size}

        lo_start = ((pages*page_size) + 1)
        lo_end = ((pages*page_size) + left_over)
        lo_key = "Page {page}, Messages [{start} - {end}]".format(page=pages+1,
            start=lo_start, end=lo_end)

        results_pages[lo_key] = {'offset': lo_start-1, 'limit': lo_end}

        menu = list(results_pages.keys())

        def onResultsMessagesPageSelected(index):
            results_pages_key = menu[index]
            results_page = results_pages[results_pages_key]
            offset = results_page['offset']
            limit = results_page['limit']
            end = offset + limit

            if ST.message_count > 0:
                ST.conn.get_job_messages(params={"results_format": "csv", "request_params":{'offset':offset, 'limit': limit}},
                     job_id=ST.search_job_id, callback=createOutput(
                        name="Job {job_id} - Messages [{start} - {end}]".format(job_id=ST.search_job_id, start=offset, end=end)))

        Window().show_quick_panel(menu, lambda index:
                                  onResultsMessagesPageSelected(index))

    @staticmethod
    def selectResultsRecordsPageQuickPanel():
        page_size = 250
        record_count = ST.record_count
        results_pages = OrderedDict()
        total = record_count
        left_over = 0
        pages = 0

        if total > 0:
            left_over = record_count % page_size
            deviadable = total - left_over
            deviadable = deviadable if deviadable > 0 else 0
            pages = int(deviadable / page_size)

        for page in range (1, pages+1):
            end = page * page_size
            start = end - (page_size-1)
            key = "Page {page}, Records [{start} - {end}]".format(page=page, start=start, end=end)
            results_pages[key] = {'offset': start-1, 'limit': page_size}

        lo_start = ((pages*page_size) + 1)
        lo_end = ((pages*page_size) + left_over)
        lo_key = "Page {page}, Records [{start} - {end}]".format(page=pages+1,
            start=lo_start, end=lo_end)

        results_pages[lo_key] = {'offset': lo_start-1, 'limit': lo_end}

        menu = list(results_pages.keys())

        def onResultsRecordsPageSelected(index):
            results_pages_key = menu[index]
            results_page = results_pages[results_pages_key]
            offset = results_page['offset']
            limit = results_page['limit']
            end = offset + limit


            if ST.record_count > 0:
                ST.conn.get_job_records(params={"results_format": ST.results_format},
                         job_id=ST.search_job_id, callback=createOutput(
                            name="Job {job_id} - Records [{start} - {end}]".format(job_id=ST.search_job_id, start=offset, end=end)))

        Window().show_quick_panel(menu, lambda index:
                                  onResultsRecordsPageSelected(index))

    @staticmethod
    def selectTimeWindowQuickPanel(callback=None):
        if not ST.conn:
            ST.selectConnectionQuickPanel(callback=lambda:
                                          Window().run_command(
                                              'st_select_query_time_window'))
            return

        if not ST.time_zone:
            ST.selectTimeZoneQuickPanel()

        menu = get_time_window_mappings_list(time_zone=ST.time_zone)

        def on_custom_time_done(custom_date_time):
            custom_datetimes = [x.strip() for x in
                                custom_date_time.split("To") if x.strip()]
            c_from_time = get_tz_specifc_ts(custom_datetimes[0])
            c_to_time = get_tz_specifc_ts(custom_datetimes[1])

            callback(c_from_time, c_to_time)

        def onTimeSelected(index, callback):
            time_key = menu[index]

            fromTime, toTime = get_query_time_window(time_key)

            custom_date_time = "{fromTime} To {toTime}".format(
                fromTime=get_tz_specifc_time(fromTime),
                toTime=get_tz_specifc_time(toTime))

            Window().show_input_panel(caption='Custom Time Window',
                                      initial_text=custom_date_time,
                                      on_done=on_custom_time_done,
                                      on_change=None, on_cancel=None)

        Window().show_quick_panel(menu, lambda index:
                                  onTimeSelected(index, callback))

    @staticmethod
    def showCollectorsQuickPanel(callback=None):
        if len(ST.collectors) == 0:
            sublime.message_dialog(
                'Your select Sumo instance has no collectors!')
            return

        ST.showQuickPanelWithSelection(ST.collectors, callback)

    @staticmethod
    def showCategoriesQuickPanel(callback=None):
        if len(ST.source_categories) == 0:
            sublime.message_dialog(
                'Your select Sumo instance has no source categories!')
            return

        ST.showQuickPanelWithSelection(ST.source_categories, callback)

    @staticmethod
    def showQuickPanelWithSelection(arrayOfValues, callback):
        w = Window()
        view = w.active_view()
        selection = view.sel()[0]

        initialText = ''
        # ignore obvious non-identifier selections
        if selection.size() <= 128:
            (row_begin, _) = view.rowcol(selection.begin())
            (row_end, _) = view.rowcol(selection.end())
            # only consider selections within same line
            if row_begin == row_end:
                initialText = view.substr(selection)

        ddl_names = []
        for item in arrayOfValues:
            if isinstance(item, dict) and 'name' in item.keys():
                ddl_names.append(item['name'])
            else:
                ddl_names.append(str(item))

        w.show_quick_panel(ddl_names, callback)
        w.run_command('insert', {'characters': initialText})
        w.run_command("select_all")

    @staticmethod
    def on_query_completions(view, prefix, locations):
        if not view.match_selector(0, 'source.sumo'):
            return None

        currentPoint = locations[0]
        start = currentPoint - len(prefix)
        sumoRegion = expand_to_paragraph(view, currentPoint)
        sumoQuery = view.substr(sumoRegion)
        sumoQueryToCursorRegion = sublime.Region(
                                             sumoRegion.begin(),
                                             currentPoint)
        sumoQueryToCursor = view.substr(sumoQueryToCursorRegion)

        return ST.completion.getAutoCompleteList(
            view, start, locations, prefix, sumoQuery,
            sumoQueryToCursor)


class StBeautifyField(TextCommand):
    @staticmethod
    def run(edit):
        ST.beautifyField(edit)


class StBeautifyAllFields(TextCommand):
    @staticmethod
    def run(edit):
        ST.beautifyAllFields(edit)


class StShowConnectionMenu(WindowCommand):
    @staticmethod
    def run():
        Window().run_command('st_select_connection')


class StSelectResultsMessagesPage(WindowCommand):
    @staticmethod
    def run():
        ST.selectResultsMessagesPageQuickPanel()


class StSelectResultsRecordsPage(WindowCommand):
    @staticmethod
    def run():
        ST.selectResultsRecordsPageQuickPanel()


class StShowAllCollectors(WindowCommand):
    @staticmethod
    def run():
        ST.showAllCollectors()


class StSelectResultsFormat(WindowCommand):
    @staticmethod
    def run():
        ST.selectResultsFormatQuickPanel()


class StSelectTimeZone(WindowCommand):
    @staticmethod
    def run():
        ST.selectTimeZoneQuickPanel()


class StShowAllFers(WindowCommand):
    @staticmethod
    def run():
        ST.showAllFers()


class StShowAllPartitions(WindowCommand):
    @staticmethod
    def run():
        ST.showAllPartitions()


class StShowAllSchedueledViews(WindowCommand):
    @staticmethod
    def run():
        ST.showAllScheduledViews()


class StShowAllUsers(WindowCommand):
    @staticmethod
    def run():
        ST.showAllUsers()


class StShowAllRoles(WindowCommand):
    @staticmethod
    def run():
        ST.showAllRoles()


class StShowAllCategories(WindowCommand):
    @staticmethod
    def run():
        ST.showCategoriesQuickPanel()


class StSelectConnection(WindowCommand):
    @staticmethod
    def run():
        ST.selectConnectionQuickPanel()


class StShowAllSources(WindowCommand):
    @staticmethod
    def run():
        ST.showAllSources()


class StShowPanelQuery(WindowCommand):
    @staticmethod
    def run():
        ST.showPanelQuery()


class StShowFerParseExpression(WindowCommand):
    @staticmethod
    def run():
        ST.showFERParseExpression()


class StShowScheduledViewQuery(WindowCommand):
    @staticmethod
    def run():
        ST.showScheduledViewQuery()


class StSelectQueryTimeWindow(WindowCommand):
    @staticmethod
    def run():
        ST.selectTimeWindowQuickPanel()


class StGetResources(WindowCommand):
    @staticmethod
    def run(api_call, api_version='v1', uri_name=None,
            parent_uri_name=None, uri_id=None, parent_uri_id=None,
            json_root='data', limit=None, offset=None):

        callback_params = {
            "api_call": api_call, "api_version": api_version,
            "uri_name": uri_name, "parent_uri_name": parent_uri_name,
            "uri_id": uri_id, "parent_uri_id": parent_uri_id,
            "json_root": json_root}

        if not ST.conn:
            ST.selectConnectionQuickPanel(callback=lambda: Window().
                                          run_command(
                                          'st_get_resources', callback_params))
        if not ST.results_format:
            ST.selectResultsFormatQuickPanel(
                 callback=lambda: Window().run_command(
                                          'st_get_resources', callback_params))

        Window().status_message(MESSAGE_RUNNING_CMD)

        params_to_deflate = {
            "api_version": api_version, "uri_name": uri_name,
            "parent_uri_name": parent_uri_name, "uri_id": uri_id,
            "parent_uri_id": parent_uri_id, "json_root": json_root, "results_format": ST.results_format}

        if parent_uri_name:
            def onItemSelected(index):
                if index < 0:
                    return None
                Window().status_message(MESSAGE_RUNNING_CMD)

                items_to_get = getattr(ST, parent_uri_name)

                params_to_deflate = {"api_version": api_version,
                                     "uri_name": uri_name,
                                     "parent_uri_name": parent_uri_name,
                                     "uri_id": uri_id,
                                     "parent_uri_id": parent_uri_id,
                                     "json_root": json_root,
                                     "results_format": ST.results_format}

                params_to_deflate["parent_uri_id"] = items_to_get[index]['id']

                return ST.conn.getRestValues(api_call=api_call,
                                             params=params_to_deflate,
                                             callback=createOutput(
                                                 name="{0}_{1}".
                                                 format(ST.conn.accessId,
                                                        uri_name),
                                                 syntax=SYNTAX_Sumo))

            ST.showCollectorsQuickPanel(callback=onItemSelected)

        def on_results_ready(resultString, params=None):
            show_results = createOutput(
                                         name="{0}_{1}".format
                                         (
                                             ST.conn.accessId, uri_name
                                         ),
                                         syntax=SYNTAX_Sumo)
            show_results(resultString)

        return ST.conn.getRestValues(api_call=api_call,
                                     params=params_to_deflate,
                                     callback=on_results_ready)


class StShowSourceCategories(WindowCommand):
    @staticmethod
    def run():
        pass


class StRefreshConnectionData(WindowCommand):
    @staticmethod
    def run():
        if not ST.conn:
            return
        ST.loadConnectionData()


class StExecuteAll(WindowCommand):
    update_connection_loading_wip = None
    results_panel_name = None

    @staticmethod
    def populate_status(job_id, outputContent, fromTime, toTime):
        print(outputContent)
        messageCount = outputContent['messageCount']
        pendingErrors = outputContent['pendingErrors']
        recordCount = outputContent.get('recordCount', 0)
        pendingWarnings = outputContent['pendingWarnings']

        pendingWarnings_msgs = []
        for pendingWarning in pendingWarnings:
            pendingWarnings_msgs += pendingWarning.split('. ')

        pendingErrors_msgs = []
        for pendingError in pendingErrors:
            pendingErrors_msgs += pendingError.split('. ')

        done_msg = "| Job with ID:{job_id} was successfuly finished".format(
                                                                job_id=job_id)
        ofmt = "%A - %d/%m/%Y %H:%M:%S UTC%z"

        num_rec_msg = "| Records #: {recordCount}".format(
                      recordCount=recordCount)

        from_time_msg = "| From Time: {fromTime}".format(
                      fromTime=get_tz_specifc_time(fromTime, ofmt=ofmt))

        to_time_msg = "| To Time: {toTime}".format(
                      toTime=get_tz_specifc_time(toTime, ofmt=ofmt))

        num_msgs_msg = "| Messages #: {messageCount}".format(
            messageCount=messageCount)

        StExecuteAll.update_connection_loading_wip(
            "|{postfix}|\n".
            format(done_msg=done_msg,
                   postfix=(" " * (141))))

        StExecuteAll.update_connection_loading_wip(
            "{done_msg}{postfix}|\n".
            format(done_msg=done_msg,
                   postfix=(" " * (142 - (len(done_msg))))))

        StExecuteAll.update_connection_loading_wip(
            "|{postfix}|\n".
            format(done_msg=done_msg,
                   postfix=(" " * (141))))

        StExecuteAll.update_connection_loading_wip(
            "{fromTime}{postfix}|\n".
            format(fromTime=from_time_msg,
                   postfix=(" " * (142 - (len(from_time_msg))))))

        StExecuteAll.update_connection_loading_wip(
            "{toTime}{postfix}|\n".
            format(toTime=to_time_msg,
                   postfix=(" " * (142 - (len(to_time_msg))))))

        StExecuteAll.update_connection_loading_wip(
            "{num_rec_msg}{postfix}|\n".
            format(num_rec_msg=num_rec_msg,
                   postfix=(" " * (142 - (len(num_rec_msg))))))

        StExecuteAll.update_connection_loading_wip(
            "{num_msgs_msg}{postfix}|\n".
            format(num_msgs_msg=num_msgs_msg,
                   postfix=(" " * (142 - (len(num_msgs_msg))))))

        for idx, pendingWarnings_msg in enumerate(pendingWarnings_msgs):
            if idx > 0:
                warnigs_msg = "|                   {pendingWarnings_msg}".format(
                        pendingWarnings_msg=pendingWarnings_msg)
                StExecuteAll.update_connection_loading_wip(
                    "{warnigs_msg}{postfix}|\n".
                    format(warnigs_msg=warnigs_msg,
                           postfix=(" " * (142 - (len(warnigs_msg))))))
            elif idx == 0:
                warnigs_msg = "| Pending Warnings: {pendingWarnings_msg}".format(
                        pendingWarnings_msg=pendingWarnings_msg)
                StExecuteAll.update_connection_loading_wip(
                    "{warnigs_msg}{postfix}|\n".
                    format(warnigs_msg=warnigs_msg,
                           postfix=(" " * (142 - (len(warnigs_msg))))))

        for idx, pendingErrors_msg in enumerate(pendingErrors_msgs):
            if idx > 0:
                errors_msg = "|.                {pendingErrors_msg}".format(
                        pendingErrors_msg=pendingErrors_msg)
                StExecuteAll.update_connection_loading_wip(
                    "{errors_msg}{postfix}|\n".
                    format(errors_msg=errors_msg,
                           postfix=(" " * (142 - (len(errors_msg))))))
            elif idx == 0:
                errors_msg = "| Pending Errors: {pendingErrors_msg}".format(
                        pendingErrors_msg=pendingErrors_msg)
                StExecuteAll.update_connection_loading_wip(
                    "{errors_msg}{postfix}|\n".
                    format(errors_msg=errors_msg,
                           postfix=(" " * (142 - (len(errors_msg))))))

        StExecuteAll.update_connection_loading_wip(
            ("+" + "-*-+" * 35) + "-+\n")

    @staticmethod
    def run(fromTime=None, toTime=None):
        if not ST.conn:
            ST.selectConnectionQuickPanel(callback=lambda:
                                          Window().run_command(
                                                'st_execute_all'))

        if not ST.results_format:
            ST.selectResultsFormatQuickPanel(callback=lambda
                                         :
                                          Window().run_command(
                                                'st_execute_all'))

        if not fromTime:
            ST.selectTimeWindowQuickPanel(callback=lambda
                                          fromTime, toTime:
                                          Window().run_command(
                                                'st_execute_all',
                                                {'fromTime': fromTime,
                                                    'toTime': toTime}))
            return
        Window().status_message(MESSAGE_RUNNING_CMD)

        def get_status(outputContent, params):
            ST.search_job_id = None
            ST.message_count = 0
            ST.record_count = 0
            message_count = outputContent['messageCount']
            record_count = outputContent.get('recordCount', 0)
            job_id = params['uri_id']
            state = outputContent['state']
            state_msg = '| ' + state

            StExecuteAll.update_connection_loading_wip = createOutput(
                name="Job {job_id} Status".format(job_id=job_id),
                syntax=SYNTAX_Sumo, show_result_on_window_rt=False)

            StExecuteAll.update_connection_loading_wip(
                ("+" + "-*-+" * 35) + "-+\n")

            StExecuteAll.update_connection_loading_wip(
                "{state_msg}{postfix}|\n".format(
                    state_msg=state_msg,
                    postfix=(" " * (142 - (len(state_msg))))))

            histogram_buckets = outputContent['histogramBuckets']

            max_start_ts = max([histogram_bucket.get('startTimestamp', 0)
                               for histogram_bucket in histogram_buckets]) \
                if len(histogram_buckets) > 0 else 0

            min_start_ts = min([histogram_bucket.get('startTimestamp', 0)
                               for histogram_bucket in histogram_buckets]) \
                if len(histogram_buckets) > 0 else 0
            query_incomplete = 'DONE' not in state \
                and "error" not in outputContent

            wip = round(((max_start_ts - min_start_ts)
                         / (toTime - fromTime))*100)

            wip = wip if query_incomplete else 100

            StExecuteAll.update_connection_loading_wip(
                        printProgressBar(wip, 100, suffix='\n', length=141))

            if(query_incomplete):
                time.sleep(1)
                ST.conn.search_job_polling(
                   params={'uri_id': job_id}, callback=get_status)
            else:
                StExecuteAll.populate_status(
                                     job_id, outputContent, fromTime, toTime)
                time.sleep(5)

                Window().run_command("hide_panel", {"panel": "output." + StExecuteAll.results_panel_name})

                ST.search_job_id = job_id
                ST.message_count = message_count
                ST.record_count = record_count

                if message_count > 0 or record_count > 0:
                    if record_count > 0:
                        Window().run_command("st_select_results_records_page")
                    elif message_count > 0:
                        Window().run_command("st_select_results_messages_page")

        def get_job_id(outputContent, params):
            job_id = outputContent['id']
            StExecuteAll.results_panel_name = "Job {job_id} Status".format(job_id=job_id)
            StExecuteAll.update_connection_loading_wip = createOutput(
                name= StExecuteAll.results_panel_name,
                syntax=SYNTAX_Sumo, show_result_on_window_rt=False)

            StExecuteAll.update_connection_loading_wip(
                ("|" + "+-*-" * 35) + "+|\n")
            ST.conn.search_job_polling(params={
                    'uri_id': job_id
                    }, callback=get_status)

        allText = View().substr(sublime.Region(0, View().size()))

        allText2 = ''.join([x for x in allText if x in string.printable])

        ST.conn.execute(params={"request_params":
                        {'query': allText2,
                            'from': fromTime,
                            'to': toTime}}, callback=get_job_id)


class StVersion(WindowCommand):
    @staticmethod
    def run():
        sublime.message_dialog('Using {0} {1}'.format(
            __package__, __version__))


class StHistory(WindowCommand):
    @staticmethod
    def run():
        if not ST.conn:
            ST.selectConnectionQuickPanel(callback=lambda:
                                          Window().run_command('st_history'))
            return

        if len(historyStore.all()) == 0:
            sublime.message_dialog('History is empty.')
            return

        def cb(index):
            if index < 0:
                return None
            return ST.conn.execute(historyStore.get(index), createOutput())

        Window().show_quick_panel(historyStore.all(), cb)


class StSaveQuery(WindowCommand):
    @staticmethod
    def run():
        query = getSelectionText()

        def cb(alias):
            queriesStore.add(alias, query)
        Window().show_input_panel('Query alias', '', cb, None, None)


class StListQueries(WindowCommand):
    @staticmethod
    def run():
        queriesList = queriesStore.all()
        if len(queriesList) == 0:
            sublime.message_dialog('No saved queries.')
            return

        options = []
        for alias, query in queriesList.items():
            options.append([str(alias), str(query)])
        options.sort()

        def cb(index):
            if index < 0:
                return None
            alias, query = options[index]
            toNewTab(query, alias)
            return
        try:
            Window().show_quick_panel(options, cb)
        except Exception:
            pass


class StRemoveSavedQuery(WindowCommand):
    @staticmethod
    def run():
        if not ST.conn:
            ST.selectConnectionQuickPanel(
                callback=lambda: Window().run_command('st_remove_saved_query'))
            return


def Window():
    return sublime.active_window()


def View():
    return Window().active_view()


def reload():
    try:
        # python 3.0 to 3.3
        import imp
        imp.reload(sys.modules[__package__ + ".SumoSwissKnifeAPI"])
        imp.reload(sys.modules[__package__ + ".SumoSwissKnifeAPI.Utils"])
        imp.reload(sys.modules[__package__ + ".SumoSwissKnifeAPI.Completion"])
        imp.reload(sys.modules[__package__ + ".SumoSwissKnifeAPI.Storage"])
        imp.reload(sys.modules[__package__ + ".SumoSwissKnifeAPI.History"])
        imp.reload(sys.modules[__package__ + ".SumoSwissKnifeAPI.Command"])
        imp.reload(sys.modules[__package__ + ".SumoSwissKnifeAPI.Connection"])
    except Exception as e:
        raise (e)

    try:
        ST.bootstrap()
    except Exception:
        pass


def plugin_loaded():

    try:
        from package_control import events

        if events.install(__name__):
            logger.info('Installed %s!' % events.install(__name__))
        elif events.post_upgrade(__name__):
            logger.info('Upgraded to %s!' % events.post_upgrade(__name__))
            sublime.message_dialog((
                '{0} was upgraded. If you have any problem, \
                just restart your Sublime Text.').format(__name__))

    except Exception:
        pass

    startPlugin()
    reload()


def plugin_unloaded():
    if plugin_logger.handlers:
        plugin_logger.handlers.pop()


def reshape():
    layout = {
    'cells': [[0, 0, 1, 1], [0, 1, 1, 2]],
    'cols': [0.0, 1.0],
    'rows': [0.0, 0.5, 1.0]
    }
    Window().run_command('set_layout', layout)
    Window().set_minimap_visible(False)

