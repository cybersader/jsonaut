import ijson
import json
from tqdm import tqdm
import pandas as pd
from typing import Any, Dict, List, Union, Optional
from jaccard_index.jaccard import jaccard_index
from utils import count_items, get_datetime, find_root_key, DynamicDictWriter, dot_notation_match, \
    replace_index_with_brackets, combine_matching_pairs, escape_csv_string, sanitize_key_name, \
    sanitize_top_level_keys, DynamicHeaderWriter
import csv


# used to flatten objects using the array and object handling parameters, along with a separator for nested stuff
def flatten(data, array_handling='stringify', object_handling='recurse', separator='.', line_break_handling='escape',
            quote_handling='double'):
    def _flatten_helper(sub_data, prefix='', explode_buffer=None):
        if explode_buffer is None:
            explode_buffer = [{}]

        if isinstance(sub_data, dict):
            if object_handling == 'stringify' and prefix:
                for item in explode_buffer:
                    item[prefix] = escape_csv_string(json.dumps(sub_data), line_break_handling)
            else:  # object_handling == 'recurse'
                for key, value in sub_data.items():
                    new_key = sanitize_key_name(f"{prefix}{separator}{key}", line_break_handling, quote_handling) \
                        if prefix else sanitize_key_name(key, line_break_handling, quote_handling)
                    explode_buffer = _flatten_helper(value, new_key, explode_buffer)
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
        elif isinstance(sub_data, list) and array_handling == 'horizontal':
            for idx, value in enumerate(sub_data):
                new_key = f"{prefix}[{idx}]"
                explode_buffer = _flatten_helper(value, new_key, explode_buffer)
        else:
            if array_handling == 'stringify' and isinstance(sub_data, list):
                sub_data = escape_csv_string(json.dumps(sub_data), line_break_handling)
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


def granular_flatten(data, search_config, search_key_match, separator='.', _array_handling='stringify',
                     _object_handling='recurse', line_break_handling='escape', quote_handling='double'):
    def _flatten_helper(sub_data, prefix='', explode_buffer=None):
        if explode_buffer is None:
            explode_buffer = [{}]

        current_config = search_config.get(search_key_match.get(prefix, ''), {})
        if isinstance(sub_data, dict):
            object_handling = current_config.get('object_handling', _object_handling)
            if object_handling == 'stringify' and prefix:
                for item in explode_buffer:
                    item[prefix] = escape_csv_string(json.dumps(sub_data), line_break_handling)
            else:  # object_handling == 'recurse'
                for key, value in sub_data.items():
                    new_key = sanitize_key_name(f"{prefix}{separator}{key}", line_break_handling, quote_handling) \
                        if prefix else sanitize_key_name(key, line_break_handling, quote_handling)
                    explode_buffer = _flatten_helper(value, new_key, explode_buffer)
        elif isinstance(sub_data, list):
            array_handling = current_config.get('array_handling', _array_handling)
            if array_handling == 'explode':
                if array_handling == 'explode':
                    if not sub_data:  # Check if the array is empty
                        pass  # ignore empty arrays or leave as blank
                    else:
                        new_buffer = []
                        for value in sub_data:
                            new_explode_buffer = [item.copy() for item in explode_buffer]
                            new_explode_buffer = _flatten_helper(value, prefix, new_explode_buffer)
                            new_buffer.extend(new_explode_buffer)
                        explode_buffer = new_buffer
            elif array_handling == 'horizontal':
                for idx, value in enumerate(sub_data):
                    new_key = f"{prefix}[{idx}]"
                    explode_buffer = _flatten_helper(value, new_key, explode_buffer)
            else:  # array_handling == 'stringify'
                for item in explode_buffer:
                    item[prefix] = escape_csv_string(json.dumps(sub_data), line_break_handling)
        else:
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


def search_and_flatten(input_obj, search_config='*', similarity_threshold=1.0, array_handling='stringify',
                       object_handling='stringify', allow_dot_notation=False, separator=".", verbose=False):
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
                            matches[new_path_list_child] = key
                        else:
                            found[new_path] = value
                            matches[search_key] = key
                else:
                    if jaccard_index(search_key, key) >= sim_thresh or (allow_dot and new_path == search_key) or \
                            (dot_notation_match(search_key, new_path)):
                        was_found = True
                        if is_list_child:
                            new_path_list_child = replace_index_with_brackets(new_path)
                            found[new_path_list_child] = value
                            matches[new_path_list_child] = key
                        else:
                            found[new_path] = value
                            matches[search_key] = key
                if not was_found:
                    if isinstance(value, dict):
                        sub_found, sub_matches = process_dict(value, search_key, new_path, allow_dot, sim_thresh)
                        found.update(sub_found)
                        matches.update(sub_matches)
                    elif isinstance(value, list):
                        sub_found, sub_matches = process_list(value, search_key, new_path, allow_dot, sim_thresh)
                        found.update(sub_found)
                        matches.update(sub_matches)
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
            print(f'RUNNING - granular keys with list of objects')
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
                                             separator=separator)
                result.extend(flattened)
            return result

        # granular keys with one object
        elif isinstance(input_obj, dict):
            if verbose:
                print(f'RUNNING - granular keys with one object')
            found = find_keys(input_obj, search_config,
                              allow_dot=search_config.get('allow_dot_notation', allow_dot_notation),
                              sim_thresh=search_config.get('similarity_threshold', similarity_threshold))
            found_object = found[0]
            found_matches = found[1]
            if verbose:
                print(f'RESULTS: {found_object}')
                print(f'MATCHES: {found_matches}')
            return granular_flatten(found_object,
                                    search_config,
                                    search_key_match=found_matches,
                                    _array_handling=search_config.get('array_handling', array_handling),
                                    _object_handling=search_config.get('object_handling', object_handling),
                                    separator=separator)

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
                                    separator=separator)
                result.extend(flattened)
            return result

        # list of keys with one object
        elif isinstance(input_obj, dict):
            search_keys = search_config
            found_object = find_keys(input_obj, search_keys, allow_dot=allow_dot_notation)[0]
            flattened = flatten(found_object,
                                array_handling=array_handling,
                                object_handling=object_handling,
                                separator=separator)
            if verbose:
                print(f'RESULTS: {flattened}')
            return flattened

    else:  # search_config == '*' (wildcard)
        return flatten(input_obj,
                       array_handling=array_handling,
                       object_handling=object_handling,
                       separator=separator)


def search_and_flatten_to_csv(*, input_json: Union[str, Dict], root_key: Optional[str] = None,
                              search_config: Union[str, Dict] = '*', delimiter: str = ",",
                              similarity_threshold: float = 1.0, array_handling: str = 'stringify',
                              object_handling: str = 'stringify', allow_dot_notation: bool = False,
                              options: Optional[Dict] = None, search_name: str, verbose: bool = False,
                              separator: str = ".", mode: str = 'normal', num_test_rows: int = None):
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

    # preprocess the json file to generate a list of the present fieldnames

    with open(input_json, 'r', encoding='utf-8') as f:
        parser = ijson.items(f, f"{root_key}.item" if root_key else 'item')
        datetime = str(get_datetime())
        if root_key:
            csv_filename = 'flattened__' + search_name + '__' + root_key + '_' + datetime + ".csv"
        else:
            csv_filename = 'flattened__' + search_name + '__' + datetime + ".csv"

        with open(csv_filename, 'w+', newline='', encoding='utf-8') as csvfile:
            # Create the DictWriter with an empty set of fieldnames
            fieldnames = set()
            writer = DynamicDictWriter(csvfile, fieldnames=fieldnames, delimiter=delimiter)
            rows_written = 0
            for obj in tqdm(parser, total=total_items, desc='Processing objects', unit=' objects', ncols=100):
                results = search_and_flatten(input_obj=obj,
                                             search_config=search_config,
                                             similarity_threshold=similarity_threshold,
                                             array_handling=array_handling,
                                             object_handling=object_handling,
                                             allow_dot_notation=allow_dot_notation,
                                             separator=separator,
                                             verbose=verbose)
                if not results:
                    continue

                # If results is a single dictionary, wrap it in a list
                if isinstance(results, dict):
                    results = [results]

                for row in results:
                    writer.writerow(row)
                    rows_written += 1
                    if verbose:
                        print(row)
                    if mode == 'test' and rows_written >= num_test_rows:
                        break
                if mode == 'test' and rows_written >= num_test_rows:
                    print(f'[+] Test row number reached')
                    break
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

# TODO function to flatten down to a certain depth
