import ijson
import json
from tqdm import tqdm
import pandas as pd
from typing import Any, Dict, List, Union, Optional
from jaccard_index.jaccard import jaccard_index
from utils import count_items, get_datetime, find_root_key, DynamicDictWriter, dot_notation_match, \
    replace_index_with_brackets, combine_matching_pairs, escape_csv_string, sanitize_key_name, \
    sanitize_top_level_keys, DynamicHeaderWriter, create_temp_array_wrapped_json
import csv
from colorama import Fore, Style, init
import os
from collections import deque
import orjson


# used to flatten objects using the array and object handling parameters, along with a separator for nested stuff
def flatten(data, array_handling='stringify', object_handling='recurse', separator='.', line_break_handling='escape',
            quote_handling='escape', max_string_length=32759, long_string_handling='truncate', quote_values=False,
            remove_quotes=False):
    def _flatten_helper(sub_data, prefix='', explode_buffer=None):
        if explode_buffer is None:
            explode_buffer = [{}]

        if isinstance(sub_data, dict):
            if object_handling == 'stringify' and prefix:
                for item in explode_buffer:
                    item[prefix] = escape_csv_string(json.dumps(sub_data, default=str), line_break_handling,
                                                     quote_handling, quote_values)
            else:  # object_handling == 'recurse'
                for key, value in sub_data.items():
                    new_key = sanitize_key_name(f"{prefix}{separator}{key}", line_break_handling, quote_handling) \
                        if prefix else sanitize_key_name(key, line_break_handling, quote_handling)
                    explode_buffer = _flatten_helper(value, new_key, explode_buffer)
        # object handling == 'explode'
        elif isinstance(sub_data, list) and array_handling == 'explode':
            if not sub_data:  # Check if the array is empty
                for item in explode_buffer:
                    break  # ignore empty arrays
            else:
                new_buffer = []
                for value in sub_data:
                    new_explode_buffer = [item.copy() for item in explode_buffer]
                    new_explode_buffer = _flatten_helper(value, prefix, new_explode_buffer)
                    new_buffer.extend(new_explode_buffer)
                explode_buffer = new_buffer
        # object handling == 'horizontal'
        elif isinstance(sub_data, list) and array_handling == 'horizontal':
            for idx, value in enumerate(sub_data):
                new_key = f"{prefix}[{idx}]"
                explode_buffer = _flatten_helper(value, new_key, explode_buffer)
        else:  # object handling == 'stringify'
            if array_handling == 'stringify' and isinstance(sub_data, list):
                sub_data = escape_csv_string(json.dumps(sub_data, default=str), line_break_handling,
                                             quote_handling, quote_values)
                if remove_quotes and sub_data.startswith('"') and sub_data.endswith('"'):
                    sub_data = sub_data[1:-1]
                if len(sub_data) > max_string_length and long_string_handling == 'truncate':
                    sub_data = sub_data[:max_string_length]

            if max_string_length is not None and len(str(sub_data)) > max_string_length:
                if long_string_handling == 'truncate':
                    sub_data = str(sub_data)[:max_string_length]
                elif long_string_handling == 'horizontal':
                    sub_data = str(sub_data)
                    sub_data_parts = [sub_data[i:i + max_string_length] for i in
                                      range(0, len(sub_data), max_string_length)]
                    for idx, value in enumerate(sub_data_parts):
                        new_key = f"{prefix}[{idx}]"
                        explode_buffer = _flatten_helper(value, new_key, explode_buffer)
                elif long_string_handling == 'explode':
                    sub_data = str(sub_data)
                    sub_data_parts = [sub_data[i:i + max_string_length] for i in
                                      range(0, len(sub_data), max_string_length)]
                    new_buffer = []
                    for value in sub_data_parts:
                        new_explode_buffer = [item.copy() for item in explode_buffer]
                        new_explode_buffer = _flatten_helper(value, prefix, new_explode_buffer)
                        new_buffer.extend(new_explode_buffer)
                    explode_buffer = new_buffer

            for item in explode_buffer:
                item[prefix] = sub_data

        return explode_buffer

    data = sanitize_top_level_keys(data, line_break_handling, quote_handling)

    if isinstance(data, list):  # input is a list of objects
        result = []
        for item in data:
            flattened_item = _flatten_helper(item)
            if flattened_item:
                result.extend(flattened_item)
    else:  # input is a single object
        result = _flatten_helper(data)

    return result


def granular_flatten_slow(data, search_config, search_key_match, separator='.', _array_handling='stringify',
                          _object_handling='recurse', line_break_handling='escape', quote_handling='escape',
                          max_string_length=32759, long_string_handling='truncate', quote_values=False,
                          remove_quotes=False):
    def _flatten_helper(sub_data, prefix='', explode_buffer=None):
        if explode_buffer is None:
            explode_buffer = [{}]

        current_config = search_config.get(search_key_match.get(prefix, ''), {})
        object_handling = current_config.get('object_handling', _object_handling)
        array_handling = current_config.get('array_handling', _array_handling)

        if isinstance(sub_data, dict):
            if object_handling == 'stringify' and prefix:
                for item in explode_buffer:
                    item[prefix] = escape_csv_string(json.dumps(sub_data, default=str), line_break_handling,
                                                     quote_handling, quote_values)
            else:  # object_handling == 'recurse'
                for key, value in sub_data.items():
                    new_key = sanitize_key_name(f"{prefix}{separator}{key}", line_break_handling, quote_handling) \
                        if prefix else sanitize_key_name(key, line_break_handling, quote_handling)
                    explode_buffer = _flatten_helper(value, new_key, explode_buffer)
        elif isinstance(sub_data, list) and array_handling == 'explode':
            if not sub_data:  # Check if the array is empty
                pass  # ignore empty arrays or leave as blank
            else:
                new_buffer = []
                for value in sub_data:
                    new_explode_buffer = [item.copy() for item in explode_buffer]
                    new_explode_buffer = _flatten_helper(value, prefix, new_explode_buffer)
                    new_buffer.extend(new_explode_buffer)
                explode_buffer = new_buffer
        elif isinstance(sub_data, list) and array_handling == 'horizontal':
            for idx, value in enumerate(sub_data):
                new_key = f"{prefix}[{idx}]"
                explode_buffer = _flatten_helper(value, new_key, explode_buffer)
        else:  # array_handling == 'stringify'
            serialized_sub_data = json.dumps(sub_data, default=str)
            if remove_quotes and serialized_sub_data.startswith('"') and serialized_sub_data.endswith('"'):
                serialized_sub_data = serialized_sub_data[1:-1]
            for item in explode_buffer:
                item[prefix] = escape_csv_string(serialized_sub_data, line_break_handling,
                                                 quote_handling, quote_values)
            if max_string_length is not None and len(str(sub_data)) > max_string_length:
                if long_string_handling == 'truncate':
                    item[prefix] = sub_data[:max_string_length]
                elif long_string_handling == 'horizontal':
                    sub_data = str(sub_data)
                    sub_data_parts = [sub_data[i:i + max_string_length] for i in
                                      range(0, len(sub_data), max_string_length)]
                    for idx, value in enumerate(sub_data_parts):
                        new_key = f"{prefix}[{idx}]"
                        explode_buffer = _flatten_helper(value, new_key, explode_buffer)
                elif long_string_handling == 'explode':
                    sub_data = str(sub_data)
                    sub_data_parts = [sub_data[i:i + max_string_length] for i in
                                      range(0, len(sub_data), max_string_length)]
                    new_buffer = []
                    for value in sub_data_parts:
                        new_explode_buffer = [item.copy() for item in explode_buffer]
                        new_explode_buffer = _flatten_helper(value, prefix, new_explode_buffer)
                        new_buffer.extend(new_explode_buffer)
                    explode_buffer = new_buffer

                    for item in explode_buffer:
                        item[prefix] = sub_data

        return explode_buffer

    data = sanitize_top_level_keys(data, line_break_handling, quote_handling)

    if isinstance(data, list):  # input is a list of objects
        result = []
        for item in data:
            flattened_item = _flatten_helper(item)
            if flattened_item:
                result.extend(flattened_item)
    else:  # input is a single object
        result = _flatten_helper(data)
    return result


def granular_flatten_still_slow(data, search_config, search_key_match, separator='.', _array_handling='stringify',
                                _object_handling='recurse', line_break_handling='escape', quote_handling='escape',
                                max_string_length=32759, long_string_handling='truncate', quote_values=False,
                                remove_quotes=False):
    def custom_dumps(obj):
        if isinstance(obj, str):
            return obj
        elif obj is None:
            return 'null'
        elif isinstance(obj, bool):
            return str(obj).lower()
        elif isinstance(obj, (int, float)):
            return str(obj)
        else:
            return json.dumps(obj, default=str)

    def _flatten_helper(sub_data, prefix='', explode_buffer=None):
        if explode_buffer is None:
            explode_buffer = [{}]

        current_config = search_config.get(search_key_match.get(prefix, ''), {})
        object_handling = current_config.get('object_handling', _object_handling)
        array_handling = current_config.get('array_handling', _array_handling)

        if isinstance(sub_data, dict):
            if object_handling == 'stringify' and prefix:
                for item in explode_buffer:
                    item[prefix] = escape_csv_string(custom_dumps(sub_data), line_break_handling,
                                                     quote_handling, quote_values)
            else:  # object_handling == 'recurse'
                for key, value in sub_data.items():
                    new_key = sanitize_key_name(f"{prefix}{separator}{key}", line_break_handling, quote_handling) \
                        if prefix else sanitize_key_name(key, line_break_handling, quote_handling)
                    explode_buffer = _flatten_helper(value, new_key, explode_buffer)
        elif isinstance(sub_data, list) and array_handling == 'explode':
            if sub_data:  # Check if the array is not empty
                new_buffer = []
                for value in sub_data:
                    new_explode_buffer = [item.copy() for item in explode_buffer]
                    new_explode_buffer = _flatten_helper(value, prefix, new_explode_buffer)
                    new_buffer.extend(new_explode_buffer)
                explode_buffer = new_buffer
        elif isinstance(sub_data, list) and array_handling == 'horizontal':
            for idx, value in enumerate(sub_data):
                new_key = f"{prefix}[{idx}]"
                explode_buffer = _flatten_helper(value, new_key, explode_buffer)
        else:  # array_handling == 'stringify'
            serialized_sub_data = json.dumps(sub_data, default=str)
            if remove_quotes and serialized_sub_data.startswith('"') and serialized_sub_data.endswith('"'):
                serialized_sub_data = serialized_sub_data[1:-1]
            for item in explode_buffer:
                item[prefix] = escape_csv_string(serialized_sub_data, line_break_handling,
                                                 quote_handling, quote_values)
            if max_string_length is not None and len(serialized_sub_data) > max_string_length:
                if long_string_handling == 'truncate':
                    item[prefix] = serialized_sub_data[:max_string_length]
                elif long_string_handling == 'horizontal':
                    sub_data_parts = [serialized_sub_data[i:i + max_string_length] for i in
                                      range(0, len(serialized_sub_data), max_string_length)]
                    for idx, value in enumerate(sub_data_parts):
                        new_key = f"{prefix}[{idx}]"
                        explode_buffer = _flatten_helper(value, new_key, explode_buffer)
                elif long_string_handling == 'explode':
                    sub_data_parts = [serialized_sub_data[i:i + max_string_length] for i in
                                      range(0, len(serialized_sub_data), max_string_length)]
                    new_buffer = []
                    for value in sub_data_parts:
                        new_explode_buffer = [item.copy() for item in explode_buffer]
                        new_explode_buffer = _flatten_helper(value, prefix, new_explode_buffer)
                        new_buffer.extend(new_explode_buffer)
                    explode_buffer = new_buffer

                    for item in explode_buffer:
                        item[prefix] = serialized_sub_data

        return explode_buffer

    data = sanitize_top_level_keys(data, line_break_handling, quote_handling)

    if isinstance(data, list):  # input is a list of objects
        result = []
        for item in data:
            flattened_item = _flatten_helper(item)
            if flattened_item:
                result.extend(flattened_item)
    else:  # input is a single object
        result = _flatten_helper(data)
    return result


def granular_flatten(data, search_config, search_key_match, separator='.', _array_handling='stringify',
                     _object_handling='recurse', line_break_handling='escape', quote_handling='escape',
                     max_string_length=32759, long_string_handling='truncate', quote_values=False,
                     remove_quotes=False):

    # TODO might need to use different approaches instead of the below functions
    json_dumps = orjson.dumps
    escape_csv_string_fn = escape_csv_string

    def _flatten_helper(sub_data, prefix='', explode_buffer=None):
        if explode_buffer is None:
            explode_buffer = deque([{}])

        current_config = search_config.get(search_key_match.get(prefix, ''), {})
        object_handling = current_config.get('object_handling', _object_handling)
        array_handling = current_config.get('array_handling', _array_handling)

        if isinstance(sub_data, dict):
            if object_handling == 'stringify' and prefix:
                for item in explode_buffer:
                    item[prefix] = escape_csv_string_fn(json_dumps(sub_data, default=str).decode('utf-8'), line_break_handling,
                                                        quote_handling, quote_values)
            else:  # object_handling == 'recurse'
                for key, value in sub_data.items():
                    new_key = sanitize_key_name(f"{prefix}{separator}{key}", line_break_handling, quote_handling) \
                        if prefix else sanitize_key_name(key, line_break_handling, quote_handling)
                    explode_buffer = _flatten_helper(value, new_key, explode_buffer)
        elif isinstance(sub_data, list) and array_handling == 'explode':
            if not sub_data:  # Check if the array is empty
                pass  # ignore empty arrays or leave as blank
            else:
                new_buffer = deque()
                for value in sub_data:
                    new_explode_buffer = deque(item.copy() for item in explode_buffer)
                    new_explode_buffer = _flatten_helper(value, prefix, new_explode_buffer)
                    new_buffer.extend(new_explode_buffer)
                explode_buffer = new_buffer
        elif isinstance(sub_data, list) and array_handling == 'horizontal':
            for idx, value in enumerate(sub_data):
                new_key = f"{prefix}[{idx}]"
                explode_buffer = _flatten_helper(value, new_key, explode_buffer)
        else:  # array_handling == 'stringify'
            serialized_sub_data = json_dumps(sub_data, default=str)
            if remove_quotes:
                if isinstance(serialized_sub_data, bytes):
                    serialized_sub_data = serialized_sub_data.decode('utf-8')
                if serialized_sub_data.startswith('"') and serialized_sub_data.endswith('"'):
                    serialized_sub_data = serialized_sub_data[1:-1]
            for item in explode_buffer:
                item[prefix] = escape_csv_string_fn(serialized_sub_data, line_break_handling,
                                                    quote_handling, quote_values)
            if max_string_length is not None and len(serialized_sub_data) > max_string_length:
                if long_string_handling == 'truncate':
                    item[prefix] = serialized_sub_data[:max_string_length]
                elif long_string_handling == 'horizontal':
                    sub_data = str(sub_data)
                    sub_data_parts = [sub_data[i:i + max_string_length] for i in
                                      range(0, len(sub_data), max_string_length)]
                    for idx, value in enumerate(sub_data_parts):
                        new_key = f"{prefix}[{idx}]"
                        explode_buffer = _flatten_helper(value, new_key, explode_buffer)
                elif long_string_handling == 'explode':
                    sub_data_parts = [serialized_sub_data[i:i + max_string_length] for i in
                                      range(0, len(serialized_sub_data), max_string_length)]
                    new_buffer = deque()
                    for value in sub_data_parts:
                        new_explode_buffer = deque(item.copy() for item in explode_buffer)
                        new_explode_buffer = _flatten_helper(value, prefix, new_explode_buffer)
                        new_buffer.extend(new_explode_buffer)
                    explode_buffer = new_buffer

        return explode_buffer

    data = sanitize_top_level_keys(data, line_break_handling, quote_handling)

    if isinstance(data, list):  # input is a list of objects
        result = deque()
        for item in data:
            flattened_item = _flatten_helper(item)
            if flattened_item:
                result.extend(flattened_item)
    else:  # input is a single object
        result = _flatten_helper(data)
    return list(result)


def search_and_flatten(input_obj, search_config='*', similarity_threshold=1.0, array_handling='stringify',
                       object_handling='stringify', allow_dot_notation=False, separator=".", verbose=False,
                       max_string_length=32750, long_string_handling='truncate', quote_handling='double',
                       quote_values=False, remove_quotes=False):
    def find_keys(item, search_configs, allow_dot=False, sim_thresh=1.0):
        def process_dict(dct, search_key, path='', allow_dot=False, sim_thresh=1.0, is_list_child=False):
            found = {}
            matches = {}
            was_found = False
            for key, value in dct.items():
                new_path = f"{path}{separator}{key}" if path else key
                if allow_dot:
                    if (new_path == search_key) or (dot_notation_match(search_key, new_path)):
                        was_found = True
                        if is_list_child:
                            new_path_list_child = replace_index_with_brackets(new_path)
                            found[new_path_list_child] = value
                            matches[new_path_list_child] = new_path  # TODO FIX
                        else:
                            found[new_path] = value
                            matches[search_key] = new_path  # TODO FIX
                else:
                    if jaccard_index(search_key, key) >= sim_thresh or (allow_dot and new_path == search_key) or \
                            (dot_notation_match(search_key, new_path)):
                        was_found = True
                        if is_list_child:
                            new_path_list_child = replace_index_with_brackets(new_path)
                            found[new_path_list_child] = value
                            matches[new_path_list_child] = new_path  # TODO FIX
                        else:
                            found[new_path] = value
                            matches[search_key] = new_path  # TODO FIX
                if not was_found:
                    if isinstance(value, dict):
                        sub_found, sub_matches = process_dict(value, search_key, new_path, allow_dot, sim_thresh)
                        found.update(sub_found)
                        matches.update(sub_matches)  # TODO FIX
                    elif isinstance(value, list):
                        sub_found, sub_matches = process_list(value, search_key, new_path, allow_dot, sim_thresh)
                        found.update(sub_found)
                        matches.update(sub_matches)  # TODO FIX
            return found, matches

        def process_list(lst, search_key, path='', allow_dot=False, sim_thresh=1.0):
            found = {}
            matches = {}
            for index, value in enumerate(lst):
                new_path = f"{path}{separator}{index}"
                if isinstance(value, dict):
                    sub_found, sub_matches = process_dict(value, search_key, new_path, allow_dot, sim_thresh,
                                                          is_list_child=True)
                    if len(sub_found) != 0:
                        found = combine_matching_pairs(found, sub_found)
                        matches.update(sub_matches)
                    else:
                        continue
                elif isinstance(value, list):
                    sub_found, sub_matches = process_list(value, search_key, new_path, allow_dot, sim_thresh)
                    found.update(sub_found)
                    matches.update(sub_matches)
            return found, matches

        if isinstance(search_configs, str):
            search_configs = {search_configs: {"allow_dot_notation": allow_dot, "similarity_threshold": sim_thresh}}
        elif isinstance(search_configs, list):
            search_configs = {key: {"allow_dot_notation": allow_dot, "similarity_threshold": sim_thresh} for key in
                              search_configs}
        if isinstance(item, dict):
            result = {}
            matches = {}
            for search_key, config in search_configs.items():
                found, key_matches = process_dict(item, search_key, allow_dot=config.get('allow_dot_notation', False),
                                                  sim_thresh=config.get('similarity_threshold', 1.0))
                result.update(found)
                matches.update(key_matches)
            return result, matches
        elif isinstance(item, list):
            result = []
            matches = {}
            for obj in item:
                transformed_obj = {}
                obj_matches = {}
                for search_key, config in search_configs.items():
                    found, key_matches = process_dict(obj, search_key,
                                                      allow_dot=config.get('allow_dot_notation', False),
                                                      sim_thresh=config.get('similarity_threshold', 1.0))
                    transformed_obj.update(found)
                    obj_matches.update(key_matches)
                result.append(transformed_obj)
                matches.update(obj_matches)
            return result, matches

    if verbose:
        print(f'\nINPUT OBJ: {input_obj}')

    # granular search config
    if isinstance(search_config, dict):
        # granular keys with list of objects
        if isinstance(input_obj, list):
            if verbose:
                print(f'\n\nRUNNING - granular keys with list of objects')
            result = []
            for temp_object in input_obj:
                found = find_keys(temp_object, search_config,
                                  allow_dot=search_config.get('allow_dot_notation', allow_dot_notation),
                                  sim_thresh=search_config.get('similarity_threshold', similarity_threshold))
                found_object = found[0]  # The returned objects which were found
                found_matches = found[1]  # Array of tuples showing (searched key : found key)

                flattened = granular_flatten(found_object,
                                             search_config,
                                             search_key_match=found_matches,
                                             _array_handling=search_config.get('array_handling', array_handling),
                                             _object_handling=search_config.get('object_handling', object_handling),
                                             separator=separator, max_string_length=max_string_length,
                                             long_string_handling=long_string_handling,
                                             quote_handling=quote_handling,
                                             quote_values=quote_values,
                                             remove_quotes=remove_quotes)
                result.extend(flattened)
            return result

        # granular keys with one object
        elif isinstance(input_obj, dict):
            if verbose:
                print(f'\n\nRUNNING - granular keys with one object')
            found = find_keys(input_obj, search_config,
                              allow_dot=search_config.get('allow_dot_notation', allow_dot_notation),
                              sim_thresh=search_config.get('similarity_threshold', similarity_threshold))
            found_object = found[0]
            found_matches = found[1]
            if verbose:
                print(f'RESULTS: {found_object}')
                print(f'MATCHES (search_key:found_key): {found_matches}')
            return granular_flatten(found_object,
                                    search_config,
                                    search_key_match=found_matches,
                                    _array_handling=search_config.get('array_handling', array_handling),
                                    _object_handling=search_config.get('object_handling', object_handling),
                                    separator=separator, max_string_length=max_string_length,
                                    long_string_handling=long_string_handling,
                                    quote_handling=quote_handling,
                                    quote_values=quote_values,
                                    remove_quotes=remove_quotes)

    # list search config
    elif isinstance(search_config, list):
        # list of keys with list of objects
        if isinstance(input_obj, list):
            if verbose:
                print(f'RUNNING - list of keys with list of objects')
            result = []
            for temp_object in input_obj:
                search_keys = search_config
                found_object = find_keys(temp_object, search_keys, allow_dot=allow_dot_notation)[0]
                if verbose:
                    print(f'FOUND OBJ: {found_object}\n')
                flattened = flatten(found_object,
                                    array_handling=array_handling,
                                    object_handling=object_handling,
                                    separator=separator, max_string_length=max_string_length,
                                    long_string_handling=long_string_handling,
                                    quote_handling=quote_handling,
                                    quote_values=quote_values,
                                    remove_quotes=remove_quotes)
                result.extend(flattened)
            return result

        # list of keys with one object
        elif isinstance(input_obj, dict):
            search_keys = search_config
            found_object = find_keys(input_obj, search_keys, allow_dot=allow_dot_notation)[0]
            flattened = flatten(found_object,
                                array_handling=array_handling,
                                object_handling=object_handling,
                                separator=separator, max_string_length=max_string_length,
                                long_string_handling=long_string_handling,
                                quote_handling=quote_handling,
                                quote_values=quote_values,
                                remove_quotes=remove_quotes)
            if verbose:
                print(f'RESULTS: {flattened}')
            return flattened

    else:  # search_config == '*' (wildcard)
        return flatten(input_obj,
                       array_handling=array_handling,
                       object_handling=object_handling,
                       separator=separator, max_string_length=max_string_length,
                       long_string_handling=long_string_handling,
                       quote_handling=quote_handling,
                       quote_values=quote_values,
                       remove_quotes=remove_quotes)


def search_and_flatten_to_csv(*, input_json: Union[str, Dict], root_key: Optional[str] = None,
                              search_config: Union[str, Dict] = '*', delimiter: str = ",",
                              similarity_threshold: float = 1.0, array_handling: str = 'stringify',
                              object_handling: str = 'stringify', allow_dot_notation: bool = False,
                              options: Optional[Dict] = None, search_name: str, verbose: bool = False,
                              separator: str = ".", mode: str = 'normal', num_test_rows: int = None,
                              max_string_length: int = 32750, long_string_handling: str = 'truncate',
                              output_format: str = 'normal', quote_handling: str = 'escape',
                              quote_values: bool = False, quoting=csv.QUOTE_NONE, escapechar: str = '\\',
                              remove_quotes: bool = True):
    if options:
        input_json = options.get('input_json', input_json)
        root_key = options.get('root_key', root_key)
        search_config = options.get('search_config', search_config)
        delimiter = options.get('delimiter', delimiter)
        similarity_threshold = options.get('similarity_threshold', similarity_threshold)
        array_handling = options.get('array_handling', array_handling)
        object_handling = options.get('object_handling', object_handling)
        allow_dot_notation = options.get('allow_dot_notation', allow_dot_notation)
        mode = options.get('mode', mode)
        num_test_rows = options.get('num_test_rows', num_test_rows)
        max_string_length = options.get('max_string_length', max_string_length)
        long_string_handling = options.get('long_string_handling', long_string_handling)
        quote_handling = options.get('quote_handling', quote_handling)
        quote_values = options.get('quote_values', quote_values)
        quoting = options.get('quoting', quoting)
        escapechar = options.get('escapechar', escapechar)
        remove_quotes = options.get('remove_quotes', remove_quotes)

    if mode == 'test' and num_test_rows is None:
        raise ValueError("num_test_rows must be provided when mode is 'test'")
    elif mode == 'test' and num_test_rows:
        print(f'[+] Test mode selected with {num_test_rows} rows')

    # Get the total number objects in the input json file (no matter how large)
    print(f'[+] Parsing -> {input_json}')

    is_array, found_root_key = find_root_key(input_json, root_key)
    if found_root_key:
        root_key = found_root_key
    if mode == 'test':
        total_items = count_items(input_json, root_key, is_array, num_test_rows)
    else:
        total_items = count_items(input_json, root_key, is_array)

    if root_key:
        file_to_use = input_json
        item_prefix = f"{root_key}.item"
    elif is_array:
        file_to_use = input_json
        item_prefix = 'item'
    else:  # TODO fix this case to work
        # If the input JSON is a singular object, create a temporary JSON file with the input wrapped in an array
        temp_input_json = create_temp_array_wrapped_json(input_json)
        print(f'FILE TO USE: {temp_input_json}')
        file_to_use = temp_input_json
        total_items = 1
        item_prefix = 'item.item'

    with open(file_to_use, 'r', encoding='utf-8') as f:

        datetime = str(get_datetime())
        if output_format == 'datetime':
            if root_key:
                csv_filename = 'flattened__' + search_name + '__' + root_key + '_' + datetime + ".csv"
            else:
                csv_filename = 'flattened__' + search_name + '__' + datetime + ".csv"
        if output_format == 'normal':
            input_json_basename = os.path.basename(input_json)
            filename_without_ext = os.path.splitext(input_json_basename)[0]
            csv_filename = f'flattened__{filename_without_ext}.csv'

        with open(csv_filename, 'w+', newline='', encoding='utf-8') as csvfile:
            parser = ijson.items(f, item_prefix)
            # Create the DictWriter with an empty set of fieldnames
            fieldnames = []
            if search_config == "*":
                # TODO improve this for wildcard option
                headers_csv = get_flattened_csv_headers_from_json(input_json=input_json, root_key=root_key, mode=mode,
                                                                  num_test_rows=num_test_rows, separator=separator)
                fieldnames = get_first_column_values(headers_csv)
                print(fieldnames)

            # TODO add dialect control at config level
            writer = DynamicDictWriter(csvfile, fieldnames=fieldnames, delimiter=delimiter, dialect='excel',
                                       quoting=quoting, escapechar=escapechar)
            if search_config == "*":
                writer.update_header()

            rows_written = 0
            for obj in tqdm(parser, total=total_items, desc='Processing objects', unit=' objects', ncols=100):
                results = search_and_flatten(input_obj=obj,
                                             search_config=search_config,
                                             similarity_threshold=similarity_threshold,
                                             array_handling=array_handling,
                                             object_handling=object_handling,
                                             allow_dot_notation=allow_dot_notation,
                                             separator=separator,
                                             verbose=verbose,
                                             max_string_length=max_string_length,
                                             long_string_handling=long_string_handling,
                                             quote_handling=quote_handling,
                                             quote_values=quote_values,
                                             remove_quotes=remove_quotes)
                if not results:
                    continue

                # If results is a single dictionary, wrap it in a list
                if isinstance(results, dict):
                    results = [results]

                for row in results:
                    # writer.smart_writerow(row)  TODO smart writer
                    writer.writerow(row)
                    rows_written += 1
                    if verbose:
                        print(f'ROW: {row}')
                    if mode == 'test' and rows_written >= num_test_rows:
                        break
                if mode == 'test' and rows_written >= num_test_rows:
                    print(f'[+] Test row number reached')
                    break
            # writer.remove_padding()  TODO smart writer
    return csv_filename


def get_flattened_csv_headers_from_json(input_json: Union[str, Dict], root_key: Optional[str] = None,
                                        delimiter: str = ",", separator: str = ".", mode: str = 'normal',
                                        num_test_rows: int = None):
    if mode == 'test' and num_test_rows is None:
        raise ValueError("num_test_rows must be provided when mode is 'test'")
    elif mode == 'test' and num_test_rows:
        print(f'[+] Test mode selected with {num_test_rows} rows')

    # Get the total number objects in the input json file (no matter how large)
    print(f'[+] Parsing -> {input_json}')
    if find_root_key(input_json):
        if mode == 'test':
            total_items = count_items(input_json, root_key, num_test_rows)
        else:
            total_items = count_items(input_json, root_key)
    else:
        print(f'[X] Root Key NOT Found for {input_json}')

    with open(input_json, 'r', encoding='utf-8') as f:
        parser = ijson.items(f, f"{root_key}.item" if root_key else 'item')
        datetime = str(get_datetime())

        if root_key:
            csv_filename = 'headers__' + root_key + '_' + datetime + ".csv"
        else:
            csv_filename = 'headers__' + datetime + ".csv"

        with open(csv_filename, 'w+', newline='', encoding='utf-8') as csvfile:
            # Create the DynamicHeaderWriter with an empty set of fieldnames
            fieldnames = set()
            writer = DynamicHeaderWriter(csvfile, fieldnames=fieldnames, delimiter=delimiter)
            rows_written = 0
            for obj in tqdm(parser, total=total_items, desc='Processing objects', unit=' objects', ncols=100):
                results = flatten(obj,
                                  array_handling='stringify',
                                  object_handling='recurse',
                                  separator=separator)
                if not results:
                    continue
                # If results is a single dictionary, wrap it in a list
                if isinstance(results, dict):
                    results = [results]
                for row in results:
                    writer.process_row(row)
                    rows_written += 1
                    if mode == 'test' and rows_written >= num_test_rows:
                        break
                if mode == 'test' and rows_written >= num_test_rows:
                    print(f'[+] Test row number reached')
                    break
            # take the csv file that should just have a header row, transpose the row, and
            # give it a header/column of "headers"
            # Read the header CSV file and create a DataFrame
            csvfile.seek(0)
            df = pd.read_csv(csvfile, header=None)

            # Transpose the DataFrame and set the column name to "headers"
            df = df.T
            df.columns = ['headers']

            # Save the transposed DataFrame back to the original headers file
            csvfile.seek(0)
            csvfile.truncate()
            df.to_csv(csvfile, index=False)

        return csv_filename

def get_first_column_values(csv_filename):
    df = pd.read_csv(csv_filename)
    first_column_values = df.iloc[:, 0].tolist() #gets the first column
    return first_column_values

# TODO function to flatten down to a certain depth
