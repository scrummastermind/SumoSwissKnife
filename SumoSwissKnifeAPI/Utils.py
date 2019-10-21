__version__ = "v0.0.1"

import json
import os
import re
import sys
from tabulate import tabulate
import sqlite3
from sqlite3 import Error
import requests
from datetime import date, datetime
from dateutil.relativedelta import relativedelta, MO
import pprint
from pytz import timezone, all_timezones
import csv
from tempfile import NamedTemporaryFile
import time
from os.path import expanduser
from collections import OrderedDict



dirpath = os.path.join(os.path.dirname(__file__), 'lib')
pp = pprint.PrettyPrinter(indent=4)

if dirpath not in sys.path:
    sys.path.append(dirpath)

# Regular expression for comments
comment_re = re.compile(
    '(^)?[^\\S\n]*/(?:\\*(.*?)\\*/[^\\S\n]*|/[^\n]*)($)?',
    re.DOTALL | re.MULTILINE
)

O_CAPS = re.compile(
    '([^A-Z]+?)(?<!\\s)([A-Z]+)')

tz = None
fmt = "%d/%m/%Y %H:%M:%S UTC%z"


def pprint(content):
    pp.pprint(content)


def parseRawJson(filename):
    json_object = None
    try:
        with open(filename, mode='r', encoding='utf-8') as f:
            content = ''.join(f.readlines())
            json_object = json.loads(content, encoding='utf-8')
    except Exception as e:
        print(e)
    return json_object


def saveRawJson(content, filename):
    with open(filename, mode='w', encoding='utf-8') as outfile:
        json.dump(content, outfile)


def parseJson(filename):
    with open(filename, mode='r', encoding='utf-8') as f:
        content = ''.join(f.readlines())

        # Looking for comments
        match = comment_re.search(content)
        while match:
            # single line comment
            content = content[:match.start()] + content[match.end():]
            match = comment_re.search(content)

        # remove trailing commas
        content = re.sub(r',([ \t\r\n]+)}', r'\1}', content)
        content = re.sub(r',([ \t\r\n]+)\]', r'\1]', content)

        # Return json file
        return json.loads(content, encoding='utf-8')


def saveJson(content, filename):
    with open(filename, mode='w', encoding='utf-8') as outfile:
        json.dump(content, outfile, indent=2, separators=(',', ': '))
    print(filename + 'done')


def getResultAsList(results):
    resultList = []
    for result in results.splitlines():
        lineResult = ''
        for element in result.strip('|').split('|'):
            lineResult += element.strip()
        if lineResult:
            resultList.append(lineResult)
    return resultList


def formatSumo(raw, settings):
    try:
        result = raw

        return result
    except Exception:
        return None


def merge(source, destination):
    for key, value in source.items():
        if isinstance(value, dict):
            # get node or create one
            node = destination.setdefault(key, {})
            merge(value, node)
        else:
            destination[key] = value

    return destination


def toTitle(phrase):
    return O_CAPS.sub('\\g<1> \\g<2>', phrase).title()


def listToTitles(list_phrases):
    return [toTitle((phrase.replace('_', '', 1)).replace('_',' ')) for phrase in list_phrases]


def jsonListToTabulate(json_data, tabulate_format):
    resultString = ''

    processed_data = []
    header = []

    orig_jdata = json_data
    list_jdata = list(json_data)
    to_process = None

    if isinstance(json_data, list):
        to_process = orig_jdata
    else:
        to_process = list_jdata

    if len(to_process) < 1 or len(to_process[0].keys()) < 1:
        return

    all_col_names = []

    for row in to_process:
        if 'map' in row.keys() and len(row.keys()) == 1:
            row = row['map']
        processed_data.append(row)
        for col_name in row.keys():
            if col_name not in all_col_names:
                all_col_names.append(col_name)

    header = all_col_names
    header = list(set(header))

    beautified_header = OrderedDict()

    for header_col in header:
        beautified_header[header_col] = toTitle((header_col.replace('_', '', 1)).replace('_', ' '))


    processed_data.insert(0, beautified_header)

    resultString = tabulate(processed_data, headers='firstrow',
                            tablefmt=tabulate_format)

    return resultString


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def get_operator_docs(operator, callback):
    url = "https://help.sumologic.com/05Search/\
    Search-Query-Language/Transaction-Analytics/\
    Transaction-Operator"
    requests.get(url, hooks={'response': callback})


def printProgressBar(iteration, total, prefix='', suffix='', decimals=1,
                     length=100, fill='▮'):
    percent = ("{0:." + str(decimals) + "f}").format(
                                             100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    return "{prefix}|{bar}| {percent}% {suffix}".format(prefix=prefix, bar=bar,
                                                        percent=percent,
                                                        suffix=suffix)


def get_ephoch():
    return datetime.fromtimestamp(0, timezone(tz))


def get_milliseconds_since(moment):
    return round((moment.replace(tzinfo=timezone(tz))
                  - get_ephoch()).total_seconds()*1000)


def get_now():
    now_utc = datetime.now(timezone('UTC'))
    now_tz = now_utc.astimezone(timezone(tz))
    return now_tz


def get_today():
    return get_now().date()


def get_ts(time_zone=None):
    return get_milliseconds_since(get_today())


def get_now_ts():
    return get_milliseconds_since(get_now())


def get_time_window_mappings(date_pattern=None):
    date_patterns = {
        'Custom':'',
        'Last 60 Seconds':
        '(get_milliseconds_since(get_now() +\
         relativedelta(seconds=-60)), get_milliseconds_since(get_now()))',
        'Last 5 Minutes':
        '(get_milliseconds_since(get_now() +\
         relativedelta(minutes=-5)), get_milliseconds_since(get_now()))',
        'Last 10 Minutes':
        '(get_milliseconds_since(get_now() +\
         relativedelta(minutes=-10)), get_milliseconds_since(get_now()))',
        'Last 15 Minutes':
        '(get_milliseconds_since(get_now() +\
         relativedelta(minutes=-15)), get_milliseconds_since(get_now()))',
        'Last 60 Minutes':
        '(get_milliseconds_since(get_now() +\
         relativedelta(minutes=-60)), get_milliseconds_since(get_now()))',
        'Last 3 Hours':
        '(get_milliseconds_since(get_now() +\
         relativedelta(hours=-3)), get_milliseconds_since(get_now()))',
        'Last 6 Hours':
        '(get_milliseconds_since(get_now() +\
         relativedelta(hours=-6)), get_milliseconds_since(get_now()))',
        'Last 24 Hours':
        '(get_milliseconds_since(get_now() +\
         relativedelta(hours=-24)), get_milliseconds_since(get_now()))',
        'Today':
        '(get_milliseconds_since(datetime.combine(get_today(), datetime.min.time())), get_milliseconds_since(datetime.combine(get_today(), datetime.max.time())))',
        'Yesterday':
        '(get_milliseconds_since(datetime.combine(get_today() + relativedelta(days=-1), datetime.min.time())), get_milliseconds_since(datetime.combine(get_today()  + relativedelta(days=-1), datetime.max.time())))',
        'Last 3 Days':
        '(get_milliseconds_since(get_now() +\
         relativedelta(days=-3)), get_milliseconds_since(get_now()))',
        'Last 7 Days':
        '(get_milliseconds_since(get_now() +\
         relativedelta(days=-7)), get_milliseconds_since(get_now()))',
        'This Week':
        '(get_milliseconds_since(datetime.combine(get_now() + relativedelta(weekday=MO(-1)),datetime.min.time())), get_milliseconds_since(get_now()))',
        'Last 14 Days':
        '(get_milliseconds_since(get_now() +\
         relativedelta(days=-14)), get_milliseconds_since(get_now()))',
        'Last 30 Days':
        '(get_milliseconds_since(get_now() +\
         relativedelta(days=-30)), get_milliseconds_since(get_now()))',
        'This Month':
        '(get_milliseconds_since(datetime.combine(get_today().replace(day=1), datetime.min.time())), get_milliseconds_since(get_now()))',
        'Previous Month':
        '(get_milliseconds_since(datetime.combine(get_today().replace(day=1) + relativedelta(months=-1), datetime.min.time())), get_milliseconds_since(datetime.combine(get_today().replace(day=1) + relativedelta(days=-1), datetime.max.time())))'
        }
    return date_patterns[date_pattern] if date_pattern else date_patterns


def get_time_window_mappings_list(time_zone=None):
    global tz
    tz = time_zone
    return [str(key) for key in get_time_window_mappings().keys()]


def get_query_time_window(date_pattern):
    return eval(get_time_window_mappings(date_pattern))


def get_all_timezones():
    return all_timezones


def get_tz_specifc_time(ts, ofmt=None):
    return datetime.fromtimestamp(ts/1000, timezone(tz)).strftime(
                                                    fmt if not ofmt else ofmt)


def get_tz_specifc_ts(date_time):
    return round(((datetime.strptime(date_time, fmt).replace(
                 tzinfo=timezone(tz)) - get_ephoch()
        ).total_seconds()) * 1000)


def get_formatted_results(root=None, results_format='json', json_raw_data={},offset=0):
    results = ''
    if results_format == 'json':
        results = json_raw_data

    if results_format == 'json_pretty':
        json_raw_data = {root: json_raw_data}
        results = json.dumps(json_raw_data, indent=4)

    if results_format == 'csv':
        results = Convertor.json_to_csv(json_raw_data, offset)

    if(results_format in
       ['plain', 'simple', 'github', 'grid', 'fancy_grid', 'pipe',
            'orgtbl', 'jira', 'presto', 'psql', 'rst', 'mediawiki',
            'moinmoin', 'youtrack', 'html', 'latex', 'latex_raw',
            'latex_booktabs', 'textile']):
        results = jsonListToTabulate(json_raw_data, results_format)

    if results != '' and isinstance(results, str):
        results += '\n'

    return results


def merge_dicts(master=None, slave=None):
    all_keys = []

    if slave:
        all_keys.extend(list(slave.keys()))

    if master:
        all_keys.extend(list(master.keys()))

    all_unique_keys=list(dict.fromkeys(all_keys))
    all_kv = {}

    for key in all_unique_keys:
        slave_val = None
        master_val = None
        if slave:
            slave_val = slave.get(key, None)
        if master:
            master_val = master.get(key, None)

        all_kv[key] = master_val if master_val else slave_val

    return all_kv


class Convertor(object):
    reduced_item = None

    @staticmethod
    def to_string(s):
        try:
            return str(s)
        except:
            return s.encode('utf-8')

    @staticmethod
    def reduce_item(key, value):

        if type(value) is list:
            i=0
            for sub_item in value:
                Convertor.reduce_item(key+'_'+Convertor.to_string(i), sub_item)
                i=i+1

        elif type(value) is dict:
            sub_keys = value.keys()
            for sub_key in sub_keys:
                Convertor.reduce_item(key+'_'+Convertor.to_string(sub_key), value[sub_key])

        else:
            Convertor.reduced_item[Convertor.to_string(key)] = Convertor.to_string(value)

    @staticmethod
    def json_to_csv(json_data, offset=0):

        if not json_data:
            return

        processed_data = []
        header = []

        orig_jdata = json_data
        list_jdata = list(json_data)
        to_process = None

        if isinstance(json_data, list):
            to_process = orig_jdata
        else:
            to_process = list_jdata

        if len(to_process) < 1 or len(to_process[0].keys()) < 1:
            return

        all_col_names = []

        for row in to_process:
            if 'map' in row.keys() and len(row.keys()) == 1:
                row = row['map']
            processed_data.append(row)
            for col_name in row.keys():
                if col_name not in all_col_names:
                    all_col_names.append(col_name)

        home = expanduser("~")
        fname = "Sumo_CSV_Results_Entries_From_{offset}_TS_{ts}.csv".format(ts=(time.time() * 1000), offset=offset)
        csv_file_path = os.path.join(home, fname)
        header = all_col_names
        header = list(set(header))


        beautified_header = OrderedDict()

        for header_col in header:
            beautified_header[header_col] = toTitle((header_col.replace('_', '', 1)).replace('_', ' '))

        with open(csv_file_path, 'w+') as output_file:
            dict_writer = csv.DictWriter(output_file,fieldnames=header, quoting=csv.QUOTE_ALL)
            dict_writer.writerow(beautified_header)
            for row in processed_data:
                if 'map' in row.keys() and len(row.keys()) == 1:
                    row = row['map']
                dict_writer.writerow(row)
        output_file.close()
        data = open(csv_file_path).read()
        os.system('open {file}'.format(file=output_file.name))

        return data
