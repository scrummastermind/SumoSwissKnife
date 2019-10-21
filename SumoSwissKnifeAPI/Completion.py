__version__ = "v0.0.1"

import logging
from collections import namedtuple
from sublime import INHIBIT_WORD_COMPLETIONS, INHIBIT_EXPLICIT_COMPLETIONS
import json

keywords_list = [
 "abs", "accum", "acos", "atan", "atan2", "backshift", "cbrt", "compare", "compose", "contains", "cos", "cosh", "count", "count_distinct", "count_distinct_approx", "count_frequent", "csv", "diff", "eval", "exp", "expm1", "extract", "fields", "fillmissing", "filter", "formatDate", "group", "hypot", "join", "json", "json auto", "keyvalue", "keyvalue auto", "kv", "limit", "log", "log10", "log1p", "logcompare", "logreduce", "lookup", "num", "outlier", "parse", "parse regex", "parse xml", "predict", "queryEndTime()", "queryStartTime()", "queryTimeRange()", "rollingstd", "round", "save", "sessionize", "signum", "sin", "sinh", "smooth", "sort", "split", "sqrt", "tan", "tanh", "toDegrees", "toDouble", "toInt", "toLong", "toNum", "toNumber", "toRadians", "toString", "top", "total", "trace", "transaction", "transactionize", "transpose", "where", "avg", "base64Decode", "base64Encode", "cat", "ceil", "compareCIDRPrefix", "concat", "decToHex", "first", "floor", "format", "getCIDRPrefix", "haversine", "hexToAscii", "hexToDec", "ipv4ToNumber", "isBlank", "isEmpty", "isNull", "isNumeric", "isPrivateIP", "isPublicIP", "isValidIP", "isValidIPv4", "isValidIPv6", "keyvalue_auto", "last", "length", "luhn", "maskFromCIDR", "max", "min", "now", "order", "parseHex", "pct", "queryEndTime", "queryStartTime", "queryTimeRange", "replace", "stddev", "substring", "sum", "summarize", "toLowerCase", "topk", "toUpperCase", "tourl", "trim", "urldecode", "urlencode", "_index", "_view", "AND", "by", "if", "int", "OR"
]

meta_fields = [
    "_blockId", "_collector", "_collectorId", "_format", "_messageCount",
    "_messageId", "_messageTime", "_raw", "_receiptTime", "_size", "_source",
    "_sourceCategory", "_sourceHost", "_sourceId", "_sourceName"
]

logger = logging.getLogger(__name__)


def _stripPrefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


class CompletionItem(namedtuple('CompletionItem', ['type', 'ident', 'contents'])):

    def prefixMatchScore(self, view, start, search, exactly=False):
        return view.match_score(start, 'source.sumo & entity.name.sourcecategory.sumo')

    def prefixMatchListScore(self, searchList, exactly=False):
        for item in searchList:
            score = self.prefixMatchScore(item, exactly)
            if score:
                return score
        return 0

    @staticmethod
    def _stringMatched(target, search, exactly):
        if exactly:
            return target == search or search == ''
        else:
            if (len(search) == 1):
                return target.startswith(search)
            return search in target


class Completion:
    def __init__(self, allCollectors, allSources,
                 allCategories, allPartitions, allSVs, allFERs, completion_list,  settings=None):
        self.allCollectors = [CompletionItem(
            'Collector',
            clctr, clctr) for clctr in allCollectors]
        self.allSources = [CompletionItem('Src Nm',
                                          src, src) for src in allSources if src]
        self.allCategories = [CompletionItem('Src Cat',
                                             cat, cat) for cat in allCategories if cat]
        self.allPartitions = [CompletionItem('Idx',
                                             part, part) for part in allPartitions if part]
        self.allSVs = [CompletionItem('SV', sv, sv) for sv in allSVs if sv]

        self.relativeFERs = {fer['scope']: fer['fieldNames']
                             for fer in allFERs}

        self.allFERs_completions = {}

        for scope, group in self.relativeFERs.items():
            allff = []
            for f in group:
                allff.append(f)
            self.allFERs_completions[scope] = [
                CompletionItem('FER', ff, ff) for ff in allff]

        combined_kw={}

        for kw in keywords_list:
            if kw in completion_list.keys():
                combined_kw[kw] = completion_list[kw]

        combined_meta={}

        for meta in meta_fields:
            if meta in completion_list.keys():
                combined_meta[meta] = completion_list[meta]

        self.allKeywords = [CompletionItem('KWD',
                                           kwd, contents) for kwd,contents in combined_kw.items()]


        self.all_meta_fields = [CompletionItem('Meta',
                                           meta, contents) for meta, contents in combined_meta.items()]

    def getCompletions(self, completionItems):

        return ([["{ident}\t({type})".format(
            ident=cat_comp_item.ident,
            type=cat_comp_item.type),
        cat_comp_item.contents] for
            cat_comp_item in completionItems], INHIBIT_WORD_COMPLETIONS | INHIBIT_EXPLICIT_COMPLETIONS)

    def getAutoCompleteList(self, view, start, locations, prefix, sumoQuery,
                            sumoQueryToCursor):

        fers_completions = []

        [fers_completions.extend(fers_completion_set) for scope, fers_completion_set in self.allFERs_completions.items() if scope in sumoQuery]

        sumo_user_fields_regions = view.find_by_selector(
            'meta.field.user.sumo')

        sumo_user_fields = [view.substr(suf_region)
                            for suf_region in sumo_user_fields_regions]

        suf_completion_items = [CompletionItem(
            'Fld', suf, suf) for suf in sumo_user_fields]

        if view.match_selector(
                locations[0], 'meta.constant.metadata.field._sourcecategory.value.sumo'):
            return self.getCompletions(self.allCategories)

        if view.match_selector(
                locations[0], 'meta.constant.metadata.field._collector.value.sumo'):
            return self.getCompletions(self.allCollectors)

        if view.match_selector(
                locations[0], 'meta.constant.metadata.field._sourceName.value.sumo'):
            return self.getCompletions(self.allSources)

        if view.match_selector(
                locations[0], 'meta.constant.metadata.field._view.value.sumo'):
            return self.getCompletions(self.allSVs)

        if view.match_selector(
                locations[0], 'meta.constant.metadata.field._index.value.sumo'):
            return self.getCompletions(self.allPartitions)

        if view.match_selector(
                locations[0], 'meta.function-call.sumo'):
            return self.getCompletions(suf_completion_items + fers_completions + self.all_meta_fields)

        return self.getCompletions(self.all_meta_fields + self.allKeywords)
