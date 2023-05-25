import json
import ijson
import re
import heapq
from typing import Any, Dict, List, Union, Optional
from io import StringIO
from tqdm import tqdm
from datetime import datetime
from dateutil import tz
import csv
import os
import tempfile
import shutil
import humanize
import jsonlines
from functools import partial
from decimal import Decimal
import warnings
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype
import ast
from jaccard_index.jaccard import jaccard_index


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(CustomJSONEncoder, self).default(obj)


def replace_index_with_brackets(new_path):
    path_parts = new_path.split('.')
    for i, part in enumerate(path_parts):
        if part.isdigit():
            path_parts[i] = '[]'
    return '.'.join(path_parts)


def combine_matching_pairs(dict1, dict2):
    combined_dict = {}

    for key in set(dict1.keys()) | set(dict2.keys()):
        if key in dict1 and key in dict2:
            combined_dict[key] = [dict1[key], dict2[key]]
        elif key in dict1:
            combined_dict[key] = dict1[key]
        elif key in dict2:
            combined_dict[key] = dict2[key]

    return combined_dict


class CustomJSONTqdm(tqdm):
    @staticmethod
    def format_meter(n, total, elapsed, rate_fmt=None, postfix=None, ncols=None, **extra_kwargs):
        rate = n / elapsed if elapsed else 0
        remaining_time = (total - n) / rate if rate and total is not None else 0
        formatted_rate = f"{rate:.2f}"
        humanized_rate = humanize.intcomma(formatted_rate)

        if total is not None:
            return f"Counting objects: {humanize.intcomma(n)}/{humanize.intcomma(total)} objects " \
                   f"[{tqdm.format_interval(elapsed)}<{tqdm.format_interval(remaining_time)}, " \
                   f"{humanized_rate} objects/s]"
        else:
            return f"Counting objects: {humanize.intcomma(n)} objects " \
                   f"[{tqdm.format_interval(elapsed)}, {humanized_rate} objects/s]"


class CustomCSVTqdm(tqdm):
    @staticmethod
    def format_meter(n, total, elapsed, rate_fmt=None, postfix=None, ncols=None, **extra_kwargs):
        rate = n / elapsed if elapsed else 0
        remaining_time = (total - n) / rate if rate and total is not None else 0
        formatted_rate = f"{rate:.2f}"
        humanized_rate = humanize.intcomma(formatted_rate)

        if total is not None:
            return f"Counting rows: {humanize.intcomma(n)}/{humanize.intcomma(total)} rows " \
                   f"[{tqdm.format_interval(elapsed)}<{tqdm.format_interval(remaining_time)}, " \
                   f"{humanized_rate} row/s]"
        else:
            return f"Counting objects: {humanize.intcomma(n)} rows " \
                   f"[{tqdm.format_interval(elapsed)}, {humanized_rate} rows/s]"


class CustomChunkedCSVTqdm(tqdm):
    def __init__(self, iterable, chunksize, *args, **kwargs):
        self.chunksize = chunksize
        super().__init__(iterable, *args, **kwargs)

    def update_to(self, b=1, bsize=None, tsize=None):
        bsize = bsize or self.chunksize
        self.update(b * bsize - self.n)

    def format_meter(self, n, total, elapsed, rate_fmt=None, postfix=None, ncols=None, **extra_kwargs):
        rate = n / elapsed if elapsed else 0
        rate *= self.chunksize
        remaining_time = (total - n) / rate if rate and total is not None else 0
        formatted_rate = f"{rate:.2f}"
        humanized_rate = humanize.intcomma(formatted_rate)

        if total is not None:
            return f"Counting rows in chunks: {humanize.intcomma(n * self.chunksize)}/{humanize.intcomma(total)} rows " \
                   f"[{tqdm.format_interval(elapsed)}<{tqdm.format_interval(remaining_time)}, " \
                   f"{humanized_rate} rows/s]"
        else:
            return f"Counting rows in chunks: {humanize.intcomma(n * self.chunksize)} rows " \
                   f"[{tqdm.format_interval(elapsed)}, {humanized_rate} rows/s]"


class CustomComparisonTqdm2(tqdm):
    @staticmethod
    def format_meter(n, total, elapsed, rate_fmt=None, postfix=None, ncols=None, **extra_kwargs):
        rate = n / elapsed if elapsed else 0
        remaining_time = (total - n) / rate if rate and total is not None else 0
        formatted_rate = f"{rate:.2f}"
        humanized_rate = humanize.intcomma(formatted_rate)

        if total is not None:
            return f"Processing comparisons: {humanize.intcomma(n)}/{humanize.intcomma(total)} comparisons " \
                   f"[{tqdm.format_interval(elapsed)}<{tqdm.format_interval(remaining_time)}, " \
                   f"{humanized_rate} comparisons/s]"
        else:
            return f"Processing comparisons: {humanize.intcomma(n)} comparisons " \
                   f"[{tqdm.format_interval(elapsed)}, {humanized_rate} comparisons/s]"


class CustomComparisonTqdm(tqdm):
    @staticmethod
    def format_meter(n, total, elapsed, rate_fmt=None, postfix=None, ncols=None, **extra_kwargs):
        percentage = n / total * 100 if total is not None else 0
        formatted_percentage = f"{percentage:.1f}"

        if total is not None:
            return f"Processing comparisons: {humanize.intcomma(n)}/{humanize.intcomma(total)} comparisons " \
                   f"({formatted_percentage}%) [{tqdm.format_interval(elapsed)}]"
        else:
            return f"Processing comparisons: {humanize.intcomma(n)} comparisons " \
                   f"[{tqdm.format_interval(elapsed)}]"


def count_items_old(json_input, root_key=None, row_limit=None):
    with open(json_input, 'r', encoding='utf-8') as f:
        if root_key:
            items = ijson.items(f, f"{root_key}.item")
        else:
            items = ijson.items(f, 'item')

        count = 0
        for _ in CustomJSONTqdm(items, unit=' objects', ncols=None):
            count += 1
            if row_limit is not None and count >= row_limit:
                break
        return count


def count_items(json_input, root_key=None, is_array=False, row_limit=None):
    with open(json_input, 'r', encoding='utf-8') as f:
        if root_key:
            items = ijson.items(f, f"{root_key}.item")
        elif is_array:
            items = ijson.items(f, 'item')
        else:
            return 1

        count = 0
        for _ in CustomJSONTqdm(items, unit=' objects', ncols=None):
            count += 1
            if row_limit is not None and count >= row_limit:
                break
        return count


def reformat_json(input_json: str = None):
    # Set up the input and output file paths
    output_path = "reformatted__" + os.path.basename(input_json)

    # Count the total number of top-level objects in the input JSON file
    total_objects = count_items(input_json)

    # Process the input JSON file and reformat it
    with open(input_json, 'r', encoding='utf-8') as input_file, \
            open(output_path, 'w', encoding='utf-8') as output_file:
        output_file.write("[\n")
        parser = ijson.items(input_file, 'item')
        for index, obj in tqdm(enumerate(parser), total=total_objects, desc="Processing objects", unit=" objects",
                               ncols=100):
            formatted_obj = json.dumps(obj, ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
            output_file.write(formatted_obj)
            if index < total_objects - 1:
                output_file.write(",\n")
        output_file.write("\n]")
        return output_path


def truncate_json_inefficient_memory(input_json: str = None, root_key: str = None, depth: int = 1):
    def truncate(obj, current_depth):
        if current_depth > depth:
            return '{}' if isinstance(obj, dict) else '[]' if isinstance(obj, list) else str(obj)

        if isinstance(obj, dict):
            return {k: truncate(v, current_depth + 1) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [truncate(v, current_depth + 1) for v in obj]
        else:
            return obj

    total_items = count_items(input_json, root_key)

    truncated_data = []

    with open(input_json, 'r', encoding='utf-8') as f:
        parser = ijson.items(f, f"{root_key}.item" if root_key else 'item')
        input_json_basename = os.path.basename(input_json)
        filename_without_ext = os.path.splitext(input_json_basename)[0]
        json_output_filename = f'truncated__{filename_without_ext}.json'

        for obj in tqdm(parser, total=total_items, desc='Processing objects', unit=' objects', ncols=100):
            truncated_obj = truncate(obj, 1)
            truncated_data.append(truncated_obj)

    with open(json_output_filename, 'w+', newline='', encoding='utf-8') as json_output:
        json.dump(truncated_data, json_output, ensure_ascii=False, indent=2, cls=CustomJSONEncoder)

    return json_output_filename


def truncate_jsonl(input_json: str = None, root_key: str = None, depth: int = 1):
    def truncate(obj, current_depth):
        if current_depth > depth:
            return '{}' if isinstance(obj, dict) else '[]' if isinstance(obj, list) else str(obj)

        if isinstance(obj, dict):
            return {k: truncate(v, current_depth + 1) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [truncate(v, current_depth + 1) for v in obj]
        else:
            return obj

    total_items = count_items(input_json, root_key)

    with open(input_json, 'r', encoding='utf-8') as f:
        parser = ijson.items(f, f"{root_key}.item" if root_key else 'item')
        input_json_basename = os.path.basename(input_json)
        filename_without_ext = os.path.splitext(input_json_basename)[0]
        json_output_filename = f'truncated__{filename_without_ext}.jsonl'

        with jsonlines.open(json_output_filename, mode='w') as json_output:
            for obj in tqdm(parser, total=total_items, desc='Processing objects', unit=' objects', ncols=100):
                truncated_obj = truncate(obj, 1)
                json_output.write(truncated_obj)

    return json_output_filename


def truncate_json(input_json: str = None, root_key: str = None, depth: int = 1):
    def truncate(obj, current_depth):
        if current_depth > depth:
            return '{}' if isinstance(obj, dict) else '[]' if isinstance(obj, list) else str(obj)

        if isinstance(obj, dict):
            return {k: truncate(v, current_depth + 1) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [truncate(v, current_depth + 1) for v in obj]
        else:
            return obj

    total_items = count_items(input_json, root_key)

    with open(input_json, 'r', encoding='utf-8') as f:
        parser = ijson.items(f, f"{root_key}.item" if root_key else 'item')
        input_json_basename = os.path.basename(input_json)
        filename_without_ext = os.path.splitext(input_json_basename)[0]
        json_output_filename = f'truncated__{filename_without_ext}.json'

        with open(json_output_filename, 'w', newline='', encoding='utf-8') as json_output:
            json_output.write('[')
            first_item = True
            for obj in tqdm(parser, total=total_items, desc='Processing objects', unit=' objects', ncols=100):
                truncated_obj = truncate(obj, 1)
                if first_item:
                    first_item = False
                else:
                    json_output.write(',')
                json.dump(truncated_obj, json_output, ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
            json_output.write(']')

    return json_output_filename


def collapse_json(input_json: str = None, root_key: str = None, depth: int = 1):
    def truncate_stats(obj, current_depth):
        if current_depth == depth:
            if isinstance(obj, dict):
                return f'{{{len(obj)} props}}'
            elif isinstance(obj, list):
                return f'[{len(obj)} props]'
        if isinstance(obj, dict):
            return {k: truncate_stats(v, current_depth + 1) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [truncate_stats(v, current_depth + 1) for v in obj]
        else:
            return obj

    input_json_basename = os.path.basename(input_json)
    filename_without_ext = os.path.splitext(input_json_basename)[0]
    json_output_filename = f'collapse__{filename_without_ext}.json'

    with open(input_json, 'r', encoding='utf-8') as f:
        first_token = next(ijson.parse(f))
        is_single_object = first_token[0] == "start_map"
        f.seek(0)

        if is_single_object:
            parser = ijson.items(f, 'item')
            total_items = count_items(input_json, root_key) if root_key else count_items(input_json)
        else:
            parser = ijson.items(f, f"{root_key}.item" if root_key else 'item')
            total_items = count_items(input_json, root_key) if root_key else count_items(input_json)

        with open(json_output_filename, 'w', newline='', encoding='utf-8') as json_output:
            if depth == 0:
                if is_single_object:
                    progress_bar = tqdm(total=1, desc='Processing objects', unit=' objects', ncols=100)
                    progress_bar.update()
                    truncated_obj = f'{{\"{total_items} props\"}}'
                else:
                    progress_bar = tqdm(total=1, desc='Processing objects', unit=' objects', ncols=100)
                    progress_bar.update()
                    truncated_obj = f'["{total_items} props"]'
                json_output.write(truncated_obj)
                return json_output_filename

            json_output.write('[')
            first_item = True
            for obj in tqdm(parser, total=total_items, desc='Processing objects', unit=' objects', ncols=100):
                truncated_obj = truncate_stats(obj, 1)
                if first_item:
                    first_item = False
                else:
                    json_output.write(',')
                json.dump(truncated_obj, json_output, ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
            json_output.write(']')

    return json_output_filename


def dot_notation_match(search_key, path):
    if search_key == path:
        return True

    search_key_parts = search_key.split('.')
    path_parts = path.split('.')

    if len(search_key_parts) != len(path_parts):
        return False

    for search_part, path_part in zip(search_key_parts, path_parts):
        if search_part == '[]':
            if not path_part.isdigit():
                return False
        elif '[' in search_part and ']' in search_part:
            field, indices = search_part.split('[')
            start, end = indices[:-1].split('-')
            if not (start.isdigit() and end.isdigit()):
                return False
            start, end = int(start), int(end)

            if not (path_part.startswith(field + '[') and path_part.endswith(']')):
                return False

            index_str = path_part[len(field) + 1:-1]
            if not index_str.isdigit():
                return False
            index = int(index_str)

            if not (start <= index <= end):
                return False
        elif search_part != path_part:
            return False
    return True


def process_item(item, options, prefix='', row=None):
    if row is None:
        row = {}

    if isinstance(item, dict):
        for k, v in item.items():
            new_key = f"{prefix}.{k}" if prefix else k
            process_item(v, options=options, prefix=new_key, row=row)
    elif isinstance(item, list):
        array_handling = options.get('arrayHandling', 'stringify')
        if array_handling == 'stringify':
            row[prefix] = str(item)
        elif array_handling == 'explode':
            # TODO: handle explode
            pass
    else:
        row[prefix] = item

    return row


def get_datetime():
    # Get the current date and time (naive)
    current_datetime_naive = datetime.now()

    # Get the system's timezone
    system_timezone = tz.tzlocal()

    # Make the current datetime timezone-aware
    current_datetime_local = current_datetime_naive.replace(tzinfo=system_timezone)

    # Format the current date and time with the system's timezone for use in a filename
    formatted_datetime = current_datetime_local.strftime('%Y-%m-%d_%H-%M-%S_%z')

    return formatted_datetime


def find_root_key_old(input_json: str):
    with open(input_json, 'r', encoding='utf-8') as f:
        parser = ijson.parse(f)

        for prefix, event, value in parser:
            if event == 'map_key' and (prefix == '' or prefix.endswith('.item')):
                return value
            else:
                break
    return None


def find_root_key(input_json: str, root_key=None):
    with open(input_json, 'r', encoding='utf-8') as f:
        parser = ijson.parse(f)
        is_array = False
        for prefix, event, value in parser:
            if event == 'start_array' and (prefix == '' or prefix.endswith('.item')):
                is_array = True
                break
            elif event == 'map_key' and (prefix == '' or prefix.endswith('.item')):
                if root_key is None or root_key == value:
                    return (is_array, value)
            else:
                break
    return (is_array, None)


# TODO fix this
def create_temp_array_wrapped_json(input_json: str, return_basename: bool = False) -> str:
    """
    Creates a temporary JSON file containing the input JSON wrapped in an array.

    Args:
        input_json (str): Path to the input JSON file.
        return_basename (bool, optional): If True, return only the filename without the path. Defaults to False.

    Returns:
        str: Path to the temporary JSON file with the input JSON wrapped in an array (or just the filename, if return_basename is True).
    """

    # Create a temporary folder in the current directory
    temp_folder = "temp"
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    with open(input_json, 'r', encoding='utf-8') as original_file:
        # Use the temporary folder for the NamedTemporaryFile
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False, dir=temp_folder) as temp_file:
            temp_file.write('[')
            shutil.copyfileobj(original_file, temp_file)
            temp_file.write(']')
            temp_file.flush()
            temp_file_path = temp_file.name

    if return_basename:
        return os.path.basename(temp_file_path)
    else:
        return temp_file_path


class DynamicDictWriter:  # TODO finish smart functions
    def __init__(self, csvfile, fieldnames, delimiter=None, dialect='excel', quoting=csv.QUOTE_NONE, escapechar='\\',
                 smart_header_padding_amount=100000):
        self.headers_written = False
        self.fieldnames = list(fieldnames)  # Ensure fieldnames is a list
        self.csvfile = csvfile
        self.delimiter = delimiter
        self.smart_header_padding_amount = smart_header_padding_amount
        self.smart_header_present = False
        self.smart_written_num = 0
        self.padding_length = 0
        self.previous_header_length = 0
        self.remaining_padding = 0

        # Create a temporary folder in the current directory
        self.temp_folder = "temp"
        if not os.path.exists(self.temp_folder):
            os.makedirs(self.temp_folder)

        if delimiter:
            # Register a new dialect with the specified delimiter
            csv.register_dialect('custom_dialect', delimiter=delimiter,
                                 quoting=quoting, escapechar=escapechar)
            self.dialect = 'custom_dialect'
        else:
            self.dialect = dialect

        self.writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect=self.dialect)
        self.headers_written = True

    def writeheader(self):
        if not self.headers_written:
            self.writer.writeheader()
            self.headers_written = True

    def writerow(self, row):
        new_fields = set(row.keys()) - set(self.fieldnames)
        new_fields_sorted = sorted(new_fields)
        if new_fields:
            self.fieldnames += new_fields_sorted
            self.update_header()
            self.update_writer()
        self.writer.writerow(row)

    def update_header(self):
        # Read the current header row from the file up to the newline character
        self.csvfile.seek(0)
        current_header = self.csvfile.readline()

        # Calculate the length of the current header row (including newline character)
        current_header_length = len(current_header)

        # Calculate the length of the new header row (including newline character)
        new_header = ','.join(self.fieldnames) + '\n'
        new_header_length = len(new_header)

        # If the new header is longer than the current header, rewrite the entire file
        if new_header_length > current_header_length:
            self.csvfile.seek(0)

            # Use the temporary folder for the NamedTemporaryFile
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=self.temp_folder, newline='',
                                             encoding='utf-8') as temp_file:

                # Write the new header to the temporary file
                temp_file.write(new_header)
                # Move the file pointer to the position right after the current header
                self.csvfile.seek(current_header_length)  # +1 gets rid of trailing newline character
                # Copy the rest of the contents from the original file to the temporary file
                shutil.copyfileobj(self.csvfile, temp_file)
                temp_file.flush()

            # Replace the original file with the temporary file
            shutil.move(temp_file.name, self.csvfile.name)

            # Re-open the original file
            self.csvfile.close()
            self.csvfile = open(self.csvfile.name, 'r+', newline='', encoding='utf-8')
            self.csvfile.seek(0, os.SEEK_END)

        # If the new header is shorter than or equal to the current header, just update the header
        else:
            # Write the new header row to the beginning of the file
            self.csvfile.seek(0)
            self.csvfile.write(new_header)

            # Move the file pointer to the end of the file
            self.csvfile.seek(0, os.SEEK_END)

        self.update_writer()

    def smart_writerow(self, row):
        new_fields = set(row.keys()) - set(self.fieldnames)
        new_fields_sorted = sorted(new_fields)
        if new_fields:
            self.fieldnames += new_fields_sorted
            self.smart_update_header()
            self.update_writer()
        self.writer.writerow(row)
        self.smart_written_num += 1

    def smart_update_header(self):
        current_header_length = self.get_current_header_length()

        new_header = ','.join(self.fieldnames) + '\n'
        new_header_length = len(new_header)

        if not self.smart_header_present:
            self.add_padding()
            self.smart_header_present = True

        if new_header_length > (current_header_length - self.padding_length):
            self.remove_padding()
            self.update_padding()
            self.add_padding()

            self.remaining_padding = self.padding_length - (new_header_length - self.previous_header_length)
            self.csvfile.seek(0)
            self.csvfile.write(new_header + ' ' * self.remaining_padding)

        self.previous_header_length = current_header_length

    def add_padding(self):
        current_header_length = self.get_current_header_length()
        new_header = ','.join(self.fieldnames) + '\n'
        new_header_length = len(new_header)
        padding = ' ' * self.smart_header_padding_amount

        with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=self.temp_folder, newline='', encoding='utf-8') \
                as temp_file:
            self.csvfile.seek(0)
            shutil.copyfileobj(self.csvfile, temp_file, current_header_length)
            temp_file.write(padding)
            temp_file.flush()

        shutil.move(temp_file.name, self.csvfile.name)
        self.csvfile.close()
        self.csvfile = open(self.csvfile.name, 'r+', newline='', encoding='utf-8')
        self.csvfile.seek(0, os.SEEK_END)

        self.padding_length = self.smart_header_padding_amount
        self.remaining_padding = self.smart_header_padding_amount

    def get_current_header_length(self):
        self.csvfile.seek(0)
        current_header = self.csvfile.readline()
        current_header_length = len(current_header)
        return current_header_length

    def update_padding(self):
        new_padding_amount = int((self.smart_written_num / self.smart_header_padding_amount) * self.padding_length)
        self.smart_header_padding_amount = max(self.smart_header_padding_amount, new_padding_amount)

    def remove_padding(self):
        if not self.smart_header_present:
            return

        self.csvfile.seek(0)
        current_header = self.csvfile.readline()
        current_header_length = len(current_header)

        self.csvfile.seek(0)
        new_header = ','.join(self.fieldnames) + '\n'
        new_header_length = len(new_header)

        padding_to_remove = current_header_length - new_header_length

        if padding_to_remove > 0:
            self.csvfile.seek(0)

            temp_folder = "temp"
            if not os.path.exists(temp_folder):
                os.makedirs(temp_folder)

            with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=temp_folder, newline='',
                                             encoding='utf-8') as temp_file:
                temp_file.write(new_header)
                self.csvfile.seek(current_header_length - padding_to_remove)
                shutil.copyfileobj(self.csvfile, temp_file)
                temp_file.flush()

            shutil.move(temp_file.name, self.csvfile.name)

            self.csvfile.close()
            self.csvfile = open(self.csvfile.name, 'r+', newline='', encoding='utf-8')
            self.csvfile.seek(0, os.SEEK_END)

            self.padding_length = 0
            self.smart_header_present = False

    def update_writer(self):
        self.writer = csv.DictWriter(self.csvfile, self.fieldnames, dialect=self.dialect)
        if not self.headers_written:
            self.writer.writeheader()


class DynamicHeaderWriter(csv.DictWriter):
    def __init__(self, csvfile, fieldnames, delimiter=None, dialect='excel'):
        self.headers_written = False
        self.fieldnames = list(fieldnames)  # Ensure fieldnames is a list
        self.csvfile = csvfile
        self.delimiter = delimiter

        if delimiter:
            # Register a new dialect with the specified delimiter
            csv.register_dialect('custom_dialect', delimiter=delimiter)
            self.dialect = 'custom_dialect'
        else:
            self.dialect = dialect

        self.writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect=self.dialect)
        self.headers_written = True

    def process_row(self, row):
        new_fields = set(row.keys()) - set(self.fieldnames)
        new_fields_sorted = sorted(new_fields)
        if new_fields:
            self.fieldnames += new_fields_sorted
            self.update_header()

    def update_header(self):
        # Read the current header row from the file up to the newline character
        self.csvfile.seek(0)
        current_header = self.csvfile.readline()

        # Calculate the length of the current header row (including newline character)
        current_header_length = len(current_header)

        # Calculate the length of the new header row (including newline character)
        new_header = ','.join(self.fieldnames) + '\n'
        new_header_length = len(new_header)

        # Move the file pointer to the beginning of the file
        self.csvfile.seek(0)

        # Write the new header row
        self.csvfile.write(new_header)

        # If the new header is shorter than the current header, fill the remaining space with spaces
        if new_header_length < current_header_length:
            padding = ' ' * (current_header_length - new_header_length)
            self.csvfile.write(padding)

        # Move the file pointer to the end of the file
        self.csvfile.seek(0, os.SEEK_END)

    def update_writer(self):
        self.writer = csv.DictWriter(self.csvfile, self.fieldnames, dialect=self.dialect)


def trim_json(input_json: Union[str, Dict], root_key: Optional[str] = None, range_str: Optional[str] = None):
    if range_str:
        start, end = map(int, range_str.split('-'))
        total_items = count_items(input_json, root_key, end)
    else:
        total_items = count_items(input_json, root_key)
        start, end = 0, total_items

    with open(input_json, 'r', encoding='utf-8') as f:
        parser = ijson.items(f, f"{root_key}.item" if root_key else 'item')
        datetime = str(get_datetime())
        json_output_filename = 'trimmed_json__' + root_key + '_' + datetime + ".json"

        with open(json_output_filename, 'w+', newline='', encoding='utf-8') as json_output:
            json_output.write("[\n")
            for idx, obj in enumerate(
                    tqdm(parser, total=total_items, desc='Processing objects', unit=' objects', ncols=100)):
                if start <= idx < end:
                    json.dump(obj, json_output, ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
                    if idx < end - 1:
                        json_output.write(",\n")
                elif idx >= end:
                    break
            json_output.write("\n]")


def bulk_rename_csv_headers(input_csv: str, rename_obj: dict, threshold: float = None):
    with open(input_csv, 'w+', newline='', encoding='utf-8') as csvfile:
        # Read the current header row from the file up to the newline character
        csvfile.seek(0)
        current_header = csvfile.readline()

        # Split the current header into columns and create a new header list
        current_columns = current_header.strip().split(',')
        new_columns = []

        for col in current_columns:
            if col in rename_obj:
                new_columns.append(rename_obj[col])
            else:
                max_similarity = -1
                best_match = col
                if threshold is not None:
                    for old_name in rename_obj:
                        similarity = jaccard_index(col, old_name)
                        if similarity > max_similarity and similarity >= threshold:
                            max_similarity = similarity
                            best_match = rename_obj[old_name]
                new_columns.append(best_match)

        new_header = ','.join(new_columns) + '\n'

        # Calculate the length of the current header row (including newline character)
        current_header_length = len(current_header)

        # Calculate the length of the new header row (including newline character)
        new_header_length = len(new_header)

        # If the new header is longer than the current header, rewrite the entire file
        if new_header_length > current_header_length:
            csvfile.seek(0)

            # Create a temporary folder in the current directory
            temp_folder = "temp"
            if not os.path.exists(temp_folder):
                os.makedirs(temp_folder)

            # Use the temporary folder for the NamedTemporaryFile
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=temp_folder, newline='',
                                             encoding='utf-8') as temp_file:

                # Write the new header to the temporary file
                temp_file.write(new_header)
                # Move the file pointer to the position right after the current header
                csvfile.seek(current_header_length)
                # Copy the rest of the contents from the original file to the temporary file
                shutil.copyfileobj(csvfile, temp_file)
                temp_file.flush()

            # Replace the original file with the temporary file
            shutil.move(temp_file.name, csvfile.name)

        # If the new header is shorter than or equal to the current header, just update the header
        else:
            # Write the new header row to the beginning of the file
            csvfile.seek(0)
            csvfile.write(new_header)

            # Move the file pointer to the end of the file
            csvfile.seek(0, os.SEEK_END)
        return input_csv


def gen_bulk_rename_csv_headers(input_csv: str, rename_obj: dict, threshold: float = None):
    with open(input_csv, 'r', newline='', encoding='utf-8') as csvfile:
        # Read the current header row from the file up to the newline character
        csvfile.seek(0)
        current_header = csvfile.readline()

        # Split the current header into columns and create a new header list
        current_columns = current_header.strip().split(',')
        new_columns = []

        for col in current_columns:
            if col in rename_obj:
                new_columns.append(rename_obj[col])
            else:
                max_similarity = -1
                best_match = col
                if threshold is not None:
                    for old_name in rename_obj:
                        similarity = jaccard_index(col, old_name)
                        if similarity > max_similarity and similarity >= threshold:
                            max_similarity = similarity
                            best_match = rename_obj[old_name]
                new_columns.append(best_match)

        new_header = ','.join(new_columns) + '\n'

        # Generate the new CSV file name with "_renamed" added to the original file name
        input_csv_name, input_csv_ext = os.path.splitext(input_csv)
        output_csv = f"{input_csv_name}_renamed{input_csv_ext}"

        # Create a new CSV file with the new header
        with open(output_csv, 'w', newline='', encoding='utf-8') as output_file:
            output_file.write(new_header)
            # Move the file pointer to the position right after the current header
            csvfile.seek(len(current_header))
            # Copy the rest of the contents from the original file to the new file
            shutil.copyfileobj(csvfile, output_file)


def escape_csv_string(s, line_break_handling='escape', quote_handling='double', quote_values=False):
    # try:
    if line_break_handling == 'escape':
        s = s.replace('\r\n', '\\r\\n').replace('\n', '\\n').replace('\r', '\\r')
    elif line_break_handling == 'remove':
        s = s.replace('\r\n', '').replace('\n', '').replace('\r', '')

    '''
    except Exception as e:
        print(f'String: {s}')
        print(f"An error occurred: {e}")
    '''

    if quote_handling == 'double':
        s = s.replace('"', '""')
    elif quote_handling == 'escape':
        s = s.replace('"', '\\"')

    if quote_values:
        s = f'"{s}"'

    return s


def sanitize_key_name(key_name, line_break_handling='escape', quote_handling='double'):
    return escape_csv_string(key_name, line_break_handling, quote_handling)


def sanitize_top_level_keys(data, line_break_handling='escape', quote_handling='double'):
    if isinstance(data, dict):
        return {sanitize_key_name(k, line_break_handling, quote_handling): v for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_top_level_keys(item, line_break_handling, quote_handling) for item in data]
    else:
        return data


def count_rows(file_path):
    with CustomCSVTqdm(pd.read_csv(file_path, iterator=True, chunksize=10000), ncols=100,
                       desc='Counting rows') as reader:
        return sum(chunk.shape[0] for chunk in reader)


def count_rows_in_chunks(file_path, chunksize):
    reader = read_csv_in_chunks(file_path, chunksize)
    with CustomChunkedCSVTqdm(reader, ncols=100, desc='Counting rows', chunksize=chunksize) as reader_tqdm:
        return sum(chunk.shape[0] for chunk in reader_tqdm)


def read_csv_in_chunks(file_path, chunksize):
    reader = pd.read_csv(file_path, iterator=True, chunksize=chunksize, low_memory=False)
    for chunk in reader:
        yield chunk


def filter_rows_by_priority_old(row_limit, input_csv, output_csv=None, filter_config=None, chunksize=10000,
                                drop_score=True, score_breakdown=False):
    def calculate_score(row):
        score = 0
        breakdown = []
        for col, config in filter_config.items():
            value = row[col]
            if isinstance(config['range'], list):
                if isinstance(config['range'][0], (int, float)):
                    min_val = config['range'][0]
                    max_val = config['range'][-1]
                    step = (max_val - min_val) / (len(config['range']) - 1)
                    normalized_value = (value - min_val) / step
                elif isinstance(config['range'][0], str):
                    index = config['range'].index(value)
                    normalized_value = index / (len(config['range']) - 1)
                else:
                    raise ValueError("Invalid value in the range list.")
            elif isinstance(config['range'], tuple) and len(config['range']) == 2:
                min_val, max_val = config['range']
                normalized_value = (value - min_val) / (max_val - min_val)
            else:
                raise ValueError("Invalid range configuration.")

            if config['order'] == 'desc':
                normalized_value = 1 - normalized_value

            score_component = normalized_value * config.get('priority', 1)
            score += score_component
            breakdown.append(f"({col}: {score_component:.2f})")

        if score_breakdown:
            return score, ' + '.join(breakdown)
        return score

    total_rows = count_rows(input_csv)

    # First pass to calculate value ranges if not provided
    for col, config in filter_config.items():
        if not config.get('range'):
            min_val = None
            max_val = None

            with tqdm(total=total_rows, desc='Calculating value ranges', unit=' rows', ncols=100) as pbar:
                for chunk in pd.read_csv(input_csv, chunksize=chunksize, usecols=[col]):
                    chunk_min, chunk_max = chunk[col].min(), chunk[col].max()
                    if min_val is None or chunk_min < min_val:
                        min_val = chunk_min
                    if max_val is None or chunk_max > max_val:
                        max_val = chunk_max
                    pbar.update(chunk.shape[0])

            config['range'] = (min_val, max_val)

    # Second pass to calculate scores and filter rows
    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8')

    header_written = False
    rows_written = 0

    with tqdm(total=total_rows, desc='Filtering rows', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(input_csv, chunksize=chunksize):
            if score_breakdown:
                chunk['score'], chunk['breakdown'] = zip(*chunk.apply(calculate_score, axis=1))
            else:
                chunk['score'] = chunk.apply(calculate_score, axis=1)
            sorted_chunk = chunk.nlargest(row_limit - rows_written, 'score')

            if not header_written:
                sorted_chunk.to_csv(temp_file, index=False)
                header_written = True
            else:
                sorted_chunk.to_csv(temp_file, index=False, header=False)

            rows_written += len(sorted_chunk)
            if rows_written >= row_limit:
                break

            pbar.update(chunk.shape[0])

    temp_file.flush()
    temp_file.close()

    if output_csv is None:
        input_csv_basename = os.path.basename(input_csv)
        filename_without_ext = os.path.splitext(input_csv_basename)[0]
        output_csv = f'custom_filtered__{filename_without_ext}.csv'

    if drop_score:
        temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8')
        with tqdm(total=row_limit, desc='Dropping score column', unit=' rows', ncols=100) as pbar:
            header_written = False
            for chunk in pd.read_csv(output_csv, chunksize=chunksize):
                chunk.drop(columns=['score'], inplace=True)
                if not header_written:
                    chunk.to_csv(temp_file, index=False)
                    header_written = True
                else:
                    chunk.to_csv(temp_file, index=False, header=False)
                pbar.update(chunk.shape[0])

        temp_file.flush()
        temp_file.close()
        shutil.move(temp_file.name, output_csv)
        return

    shutil.move(temp_file.name, output_csv)


def filter_rows_by_priority_prev(row_limit, input_csv, output_csv=None, filter_config=None, chunksize=10000,
                                 drop_score=True, score_breakdown=True):
    def calculate_score(row, score_breakdown=False):
        score = 0
        breakdown = []

        for col, config in filter_config.items():
            value = row[col]
            if isinstance(config['range'], list):
                if isinstance(config['range'][0], dict):
                    value_weights = {item['value']: item['weight'] for item in config['range']}
                    weight = value_weights.get(value, 1)
                    index = [item['value'] for item in config['range']].index(value)
                    normalized_value = index / (len(config['range']) - 1)
                    normalized_value *= weight
                elif isinstance(config['range'][0], (int, float)):
                    min_val = config['range'][0]
                    max_val = config['range'][-1]
                    step = (max_val - min_val) / (len(config['range']) - 1)
                    normalized_value = (value - min_val) / step
                elif isinstance(config['range'][0], str):
                    index = config['range'].index(value)
                    normalized_value = index / (len(config['range']) - 1)
                else:
                    raise ValueError("Invalid value in the range list.")
            elif isinstance(config['range'], tuple) and len(config['range']) == 2:
                min_val, max_val = config['range']
                normalized_value = (value - min_val) / (max_val - min_val)
            else:
                raise ValueError("Invalid range configuration.")

            if config['order'] == 'desc':
                normalized_value = 1 - normalized_value

            partial_score = normalized_value * config['priority']
            score += partial_score

            if score_breakdown:
                breakdown.append(
                    f"{col}: {value} ({normalized_value:.2f} * {config['priority']} = {partial_score:.2f})")

        if score_breakdown:
            return score, "; ".join(breakdown)

        return score

    total_rows = count_rows_in_chunks(input_csv, chunksize)

    # First pass to calculate value ranges if not provided
    for col, config in filter_config.items():
        if not config.get('range'):
            min_val = None
            max_val = None

            with tqdm(total=total_rows, desc='Calculating value ranges', unit=' rows', ncols=100) as pbar:
                for chunk in pd.read_csv(input_csv, chunksize=chunksize, usecols=[col]):
                    chunk_min, chunk_max = chunk[col].min(), chunk[col].max()
                    if min_val is None or chunk_min < min_val:
                        min_val = chunk_min
                    if max_val is None or chunk_max > max_val:
                        max_val = chunk_max
                    pbar.update(chunk.shape[0])

            config['range'] = (min_val, max_val)

    # Second pass to calculate scores and filter rows
    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8')

    header_written = False
    rows_written = 0

    with tqdm(total=total_rows, desc='Filtering rows', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(input_csv, chunksize=chunksize):
            if score_breakdown:
                chunk['score'], chunk['score_breakdown'] = zip(
                    *chunk.apply(lambda row: calculate_score(row, score_breakdown=True), axis=1))
            else:
                chunk['score'] = chunk.apply(calculate_score, axis=1)
            sorted_chunk = chunk.nlargest(row_limit - rows_written, 'score')

            if not header_written:
                sorted_chunk.to_csv(temp_file, index=False)
                header_written = True
            else:
                sorted_chunk.to_csv(temp_file, index=False, header=False)

            rows_written += len(sorted_chunk)
            if rows_written >= row_limit:
                break

            pbar.update(chunk.shape[0])

    temp_file.flush()
    temp_file.close()

    if output_csv is None:
        input_csv_basename = os.path.basename(input_csv)
        filename_without_ext = os.path.splitext(input_csv_basename)[0]
        output_csv = f'custom_filtered__{filename_without_ext}.csv'

    if drop_score:
        temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8')
        with tqdm(total=row_limit, desc='Dropping score column', unit=' rows', ncols=100) as pbar:
            header_written = False
            for chunk in pd.read_csv(output_csv, chunksize=chunksize):
                chunk.drop(columns=['score'], inplace=True)
                if not header_written:
                    chunk.to_csv(temp_file, index=False)
                    header_written = True
                else:
                    chunk.to_csv(temp_file, index=False, header=False)
                pbar.update(chunk.shape[0])

        temp_file.flush()
        temp_file.close()
        shutil.move(temp_file.name, output_csv)
        return output_csv

    shutil.move(temp_file.name, output_csv)


def filter_rows_by_priority_pandas(row_limit, input_csv, output_csv=None, filter_config=None, chunksize=10000,
                                   drop_score=True, score_breakdown=True):
    def calculate_score(row, score_breakdown=False):
        score = 0
        breakdown = []

        for col, config in filter_config.items():
            value = row[col]
            if isinstance(config['range'], list):
                if isinstance(config['range'][0], dict):
                    value_weights = {item['value']: item['weight'] for item in config['range']}
                    weight = value_weights.get(value, 1)
                    index = [item['value'] for item in config['range']].index(value)
                    normalized_value = index / (len(config['range']) - 1)
                    normalized_value *= weight
                elif isinstance(config['range'][0], (int, float)):
                    min_val = config['range'][0]
                    max_val = config['range'][-1]
                    step = (max_val - min_val) / (len(config['range']) - 1)
                    normalized_value = (value - min_val) / step
                elif isinstance(config['range'][0], str):
                    index = config['range'].index(value)
                    normalized_value = index / (len(config['range']) - 1)
                else:
                    raise ValueError("Invalid value in the range list.")
            elif isinstance(config['range'], tuple) and len(config['range']) == 2:
                min_val, max_val = config['range']
                normalized_value = (value - min_val) / (max_val - min_val)
            else:
                raise ValueError("Invalid range configuration.")

            if config['order'] == 'desc':
                normalized_value = 1 - normalized_value

            partial_score = normalized_value * config['priority']
            score += partial_score

            if score_breakdown:
                breakdown.append(
                    f"{col}: {value} ({normalized_value:.2f} * {config['priority']} = {partial_score:.2f})")

        if score_breakdown:
            return score, "; ".join(breakdown)

        return score

    total_rows = count_rows_in_chunks(input_csv, chunksize)

    # First pass to calculate value ranges if not provided
    for col, config in filter_config.items():
        if not config.get('range'):
            min_val = None
            max_val = None

            for chunk in pd.read_csv(input_csv, chunksize=chunksize, usecols=[col]):
                chunk_min, chunk_max = chunk[col].min(), chunk[col].max()
                if min_val is None or chunk_min < min_val:
                    min_val = chunk_min
                if max_val is None or chunk_max > max_val:
                    max_val = chunk_max

            config['range'] = (min_val, max_val)

    # Second pass to calculate scores and filter rows
    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8')
    header_written = False
    rows_written = 0
    with tqdm(total=total_rows, desc='Filtering rows', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            if score_breakdown:
                chunk['score'], chunk['score_breakdown'] = zip(
                    *chunk.apply(lambda row: calculate_score(row, score_breakdown=True), axis=1))
            else:
                chunk['score'] = chunk.apply(calculate_score, axis=1)

            # Sort the chunk by the 'score' column in descending order
            chunk.sort_values(by='score', ascending=False, inplace=True)

            # Select the top rows up to the "row_limit"
            if rows_written + len(chunk) > row_limit:
                chunk = chunk.iloc[:row_limit - rows_written]

            if not header_written:
                chunk.to_csv(temp_file, index=False)
                header_written = True
            else:
                chunk.to_csv(temp_file, index=False, header=False)

            rows_written += len(chunk)
            if rows_written >= row_limit:
                break

            pbar.update(chunk.shape[0])

    temp_file.flush()
    temp_file.close()

    if output_csv is None:
        input_csv_basename = os.path.basename(input_csv)
        filename_without_ext = os.path.splitext(input_csv_basename)[0]
        output_csv = f'custom_filtered__{filename_without_ext}.csv'

    if drop_score:
        temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8')
        with tqdm(total=row_limit, desc='Dropping score column', unit=' rows', ncols=100) as pbar:
            header_written = False
            for chunk in pd.read_csv(output_csv, chunksize=chunksize):
                chunk.drop(columns=['score'], inplace=True)
                if not header_written:
                    chunk.to_csv(temp_file, index=False)
                    header_written = True
                else:
                    chunk.to_csv(temp_file, index=False, header=False)
                pbar.update(chunk.shape[0])

        temp_file.flush()
        temp_file.close()

    shutil.move(temp_file.name, output_csv)
    return output_csv


def sort_big_csv(input_csv, output_path, sort_key, ascending=False, chunksize=10000):
    def process_chunk(chunk):
        sorted_chunk = chunk.sort_values(by=sort_key, ascending=ascending)
        return sorted_chunk.to_records(index=False)

    total_rows = count_rows_in_chunks(input_csv, chunksize)
    sorted_chunks = []

    with tqdm(total=total_rows, desc='Sorting chunks', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            sorted_chunks.append(process_chunk(chunk))
            pbar.update(len(chunk))

    merged_chunks = heapq.merge(*sorted_chunks, key=lambda x: (x[sort_key],), reverse=not ascending)

    with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = None
        for row in tqdm(merged_chunks, total=total_rows, desc='Merging chunks', unit=' rows', ncols=100):
            if writer is None:
                writer = csv.DictWriter(outfile, fieldnames=row.dtype.names)
                writer.writeheader()
            writer.writerow(dict(zip(row.dtype.names, row)))

    return output_path


# TODO change to use dynamicdictwriter
def sort_big_csv2(input_csv, output_path=None, sort_key=None, ascending=True, chunksize=10000):
    # If output path not given, use the input_csv path and append "_sorted" before the extension
    if output_path is None:
        output_path = os.path.splitext(input_csv)[0] + "_sorted" + os.path.splitext(input_csv)[1]

    total_rows = count_rows_in_chunks(input_csv, chunksize)  # Assuming count_rows_in_chunks is defined elsewhere

    # Open the output CSV file in write mode
    with open(output_path, mode='w+', newline='', encoding='utf-8') as csvfile:

        # Initialize DynamicDictWriter
        fieldnames = set()  # Start with no fieldnames; DynamicDictWriter will add them as they appear
        writer = DynamicDictWriter(csvfile, fieldnames=fieldnames, delimiter=',', dialect='excel')

        # Keep track of the number of rows written
        rows_written = 0

        # Read and sort the CSV in chunks
        for chunk in tqdm(pd.read_csv(input_csv, chunksize=chunksize), total=total_rows, desc='Processing chunks',
                          unit=' chunks', ncols=100):
            # If a sort key is provided, sort the chunk
            if sort_key:
                chunk.sort_values(by=sort_key, inplace=True, ascending=ascending)

            # Iterate over the rows in the chunk
            for row in chunk.to_dict('records'):
                # Write the row to the CSV
                writer.writerow(row)
                rows_written += 1

    # Return the path to the sorted CSV
    return output_path


def filter_rows_by_priority(row_limit, input_csv, output_csv=None, filter_config=None, chunksize=10000,
                            drop_score=True, score_breakdown=True, drop_below=None):
    def calculate_score2(row, score_breakdown=False):
        score = 0
        breakdown = []
        partial_scores = {}
        for col, config in filter_config.items():
            value = row[col]
            empty_value = pd.isna(value)
            order = config.get('order', 'desc')
            is_desc = True if order else False
            drop_zero = config.get('drop_zero', False)
            range = config.get('range', '')
            if_empty = config.get('if_empty', 0)
            if_not_in_range = config.get('if_not_in_range', 0)

            if (isinstance(value, str) or pd.isna(
                    value)) and range == "" and if_empty == 0 and if_not_in_range == 0 and drop_zero:
                # Add this condition to handle string values
                if empty_value:
                    normalized_value = if_empty
                else:
                    normalized_value = 1
            else:
                if empty_value:
                    normalized_value = if_empty
                else:
                    if isinstance(range, list):
                        if isinstance(range[0], dict):
                            value_weights = {item['value']: item['weight'] for item in range}
                            weight = value_weights.get(value, 1)
                            index = [item['value'] for item in range].index(value)
                            normalized_value = index / (len(range) - 1)
                            normalized_value *= weight
                        elif isinstance(range[0], (int, float)) and isinstance(range[1], (int, float)) \
                                and len(range) == 2:
                            min_val, max_val = range
                            range_is_desc = True if min_val > max_val else False
                            normalized_value = (value - min_val) / (max_val - min_val)
                            if range_is_desc == is_desc:
                                normalized_value = 1 - normalized_value
                        elif isinstance(range[0], str):
                            if len(range) == 1:
                                normalized_value = 1 if value == range[0] else if_not_in_range
                            else:
                                if value in range:
                                    index = range.index(value)
                                    normalized_value = index / (len(range) - 1)
                                else:
                                    normalized_value = if_not_in_range
                        else:
                            raise ValueError("Invalid value in the range list.")
                    else:
                        raise ValueError("Invalid range configuration.")

            partial_score = normalized_value * config['priority']
            partial_scores[col] = partial_score

            if not drop_zero or partial_score != 0:
                score += partial_score

            if score_breakdown:
                breakdown.append(
                    f"{col}: {value} ({normalized_value:.2f} * {config['priority']} = {partial_score:.2f}) || "
                    f"IS_DESC=={is_desc}")

        if score_breakdown:
            return score, "; ".join(breakdown), partial_scores

        return score, None, partial_scores

    def calculate_score(row, score_breakdown=False):
        score = 0
        breakdown = []
        partial_scores = {}
        for col, config in filter_config.items():
            value = row[col]
            empty_value = pd.isna(value)
            order = config.get('order', 'desc')
            is_desc = True if order else False
            drop_zero = config.get('drop_zero', False)
            range = config.get('range', '')
            if_empty = config.get('if_empty', 0)
            if_not_in_range = config.get('if_not_in_range', 0)

            if order not in ['asc', 'desc']:
                if isinstance(range, list):
                    if isinstance(range[0], (int, float)) and isinstance(range[1], (int, float)):
                        # if the range is numerical
                        normalized_value = 1 if min(range) <= value <= max(range) else if_not_in_range
                    elif isinstance(range[0], str):
                        # if the range is categorical
                        normalized_value = 1 if value in range else if_not_in_range
                    else:
                        raise ValueError("Invalid value in the range list.")
                else:
                    raise ValueError("Invalid range configuration.")
            else:
                if (isinstance(value, str) or pd.isna(
                        value)) and range == "" and if_empty == 0 and if_not_in_range == 0 and drop_zero:
                    if empty_value:
                        normalized_value = if_empty
                    else:
                        normalized_value = 1
                else:
                    if isinstance(range, list):
                        if isinstance(range[0], dict):
                            value_weights = {item['value']: item['weight'] for item in range}
                            weight = value_weights.get(value, 1)
                            index = [item['value'] for item in range].index(value) + 1
                            normalized_value = index / len(range)
                            normalized_value *= weight
                        elif isinstance(range[0], (int, float)) and isinstance(range[1], (int, float)) \
                                and len(range) == 2:
                            min_val, max_val = range
                            range_is_desc = True if min_val > max_val else False
                            normalized_value = (value - min_val) / (max_val - min_val)
                            if range_is_desc == is_desc:
                                normalized_value = 1 - normalized_value
                        elif isinstance(range[0], str):
                            if len(range) == 1:
                                normalized_value = 1 if value == range[0] else if_not_in_range
                            else:
                                if value in range:
                                    index = range.index(value) + 1
                                    normalized_value = index / len(range)
                                else:
                                    normalized_value = if_not_in_range
                        else:
                            raise ValueError("Invalid value in the range list.")
                    else:
                        raise ValueError("Invalid range configuration.")

            partial_score = normalized_value * config['priority']
            partial_scores[col] = partial_score

            if not drop_zero or partial_score != 0:
                score += partial_score

            if score_breakdown:
                breakdown.append(
                    f"{col}: {value} ({normalized_value:.2f} * {config['priority']} = {partial_score:.2f}) || "
                    f"IS_DESC=={is_desc}")

        if score_breakdown:
            return score, "; ".join(breakdown), partial_scores

        return score, None, partial_scores

    total_rows = count_rows_in_chunks(input_csv, chunksize)

    # First pass to calculate value ranges if not provided
    for col, config in filter_config.items():
        if not config.get('range'):
            if pd.read_csv(input_csv, nrows=5, usecols=[col])[col].dtype == object:
                # Handle string or categorical columns
                config['range'] = ""
            else:
                # Handle numerical columns
                min_val = None
                max_val = None

                for chunk in pd.read_csv(input_csv, chunksize=chunksize, usecols=[col]):
                    chunk_min, chunk_max = chunk[col].min(), chunk[col].max()
                    if min_val is None or chunk_min < min_val:
                        min_val = chunk_min
                    if max_val is None or chunk_max > max_val:
                        max_val = chunk_max

                config['range'] = (min_val, max_val)

    # Second pass to calculate scores
    temp_file_with_scores = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8')
    header_written = False
    scores_df = pd.DataFrame()

    with tqdm(total=total_rows, desc='Calculating scores', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            if score_breakdown:
                chunk['score'], chunk['score_breakdown'], chunk['partial_scores'] = zip(
                    *chunk.apply(lambda row: calculate_score(row, score_breakdown=True), axis=1))
            else:
                chunk['score'] = chunk.apply(lambda row: calculate_score(row)[0], axis=1)

            if not header_written:
                chunk.to_csv(temp_file_with_scores, index=False)
                header_written = True
            else:
                chunk.to_csv(temp_file_with_scores, index=False, header=False)

            pbar.update(chunk.shape[0])

    temp_file_with_scores.flush()
    temp_file_with_scores.close()

    # Third pass to sort rows by score and select the top rows up to the "row_limit"
    temp_file_sorted = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8')
    output_csv_with_scores = sort_big_csv(input_csv=temp_file_with_scores.name, output_path=temp_file_sorted.name,
                                          sort_key='score', ascending=False, chunksize=chunksize)
    temp_file_sorted.close()

    # Remove the temp_file_with_scores since it's no longer needed
    os.remove(temp_file_with_scores.name)

    # Read the sorted CSV and apply the row limit
    temp_file_limited = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8')
    header_written = False
    rows_written = 0

    counter = 0
    temp_total = row_limit if (row_limit < total_rows) else total_rows
    with tqdm(total=temp_total, desc='Applying row limit', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(output_csv_with_scores, chunksize=chunksize, low_memory=False):

            drop_zero_lambda = lambda row: all([
                (not filter_config[col].get('drop_zero', False)) or (calculate_score(row)[2][col] != 0)
                for col in filter_config.keys()
            ])

            drop_below_lambda = lambda row: all([
                (drop_below is None) or
                (isinstance(drop_below, (int, float)) and calculate_score(row)[2][col] >= drop_below)
                for col in filter_config.keys()
            ])

            chunk = chunk[chunk.apply(lambda row: drop_zero_lambda(row) and drop_below_lambda(row), axis=1)]

            if rows_written + len(chunk) > row_limit:
                chunk = chunk.iloc[:row_limit - rows_written]

            if not header_written:
                chunk.to_csv(temp_file_limited, index=False)
                header_written = True
            else:
                chunk.to_csv(temp_file_limited, index=False, header=False)

            rows_written += len(chunk)
            if rows_written >= row_limit:
                break

            pbar.update(chunksize)
            counter += chunk.shape[0]
            pbar.set_postfix({'Filtered Rows': counter})  # Update the filtered row count in the progress bar
            tqdm.write(f'Filtered Rows: {counter}')  # Overwrite the previous filtered row count

    temp_file_limited.flush()
    temp_file_limited.close()
    os.remove(output_csv_with_scores)  # Remove the intermediate output CSV with scores

    # Rest of the function
    if output_csv is None:
        input_csv_basename = os.path.basename(input_csv)
        filename_without_ext = os.path.splitext(input_csv_basename)[0]
        output_csv = f'custom_filtered__{filename_without_ext}.csv'

    if drop_score:
        temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8')
        with tqdm(total=row_limit, desc='Dropping score column', unit=' rows', ncols=100) as pbar:
            header_written = False
            for chunk in pd.read_csv(temp_file_limited.name, chunksize=chunksize):
                chunk.drop(columns=['score'], inplace=True)
                if not header_written:
                    chunk.to_csv(temp_file, index=False)
                    header_written = True
                else:
                    chunk.to_csv(temp_file, index=False, header=False)
                pbar.update(chunk.shape[0])

        temp_file.flush()
        temp_file.close()
        shutil.move(temp_file.name, output_csv)
    else:
        shutil.move(temp_file_limited.name, output_csv)

    return output_csv


def unique_values_with_counts_chunked(input_csv, column_names, output_csv=None, chunksize=5000):
    total_rows = count_rows(input_csv)

    if isinstance(column_names, str):
        column_names = [column_names]

    result = pd.DataFrame(columns=['column_name', 'value', 'count'])

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            for column_name in column_names:
                unique_counts = chunk[column_name].value_counts().reset_index()
                unique_counts.columns = ['value', 'count']
                unique_counts.insert(0, 'column_name', column_name)
                result = result.append(unique_counts, ignore_index=True).groupby(
                    ['column_name', 'value']).sum().reset_index()

            pbar.update(chunk.shape[0])

    if output_csv is None:
        input_csv_basename = os.path.basename(input_csv)
        filename_without_ext = os.path.splitext(input_csv_basename)[0]
        output_csv = f'uniq_vals__{filename_without_ext}.csv'

    result.to_csv(output_csv, index=False)


def explode_long_values(series, max_value_length):
    exploded_rows = []
    for idx, value in series.iteritems():
        if len(value) > max_value_length:
            value_parts = [value[i:i + max_value_length] for i in range(0, len(value), max_value_length)]
            exploded_rows.extend([(idx, part) for part in value_parts])
        else:
            exploded_rows.append((idx, value))

    return pd.DataFrame(exploded_rows, columns=['original_index', series.name])


def generate_column_analytics_in_chunks(input_csv, output_csv=None, chunksize=10000,
                                        max_value_length=5000, long_value_handling='truncate',
                                        show_unique_values=True, show_unique_counts=True,
                                        uniq_value_mode='efficient', nonnull_threshold=0.96,
                                        efficient_mode_multiplier=5):
    total_rows = count_rows(input_csv)

    aggregated_analytics = {}  # Use a dictionary to store aggregated analytics
    unique_values_sets = {}  # Initialize the unique_values_sets dictionary

    result = pd.DataFrame()

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            chunk_stats = []

            for column_name in chunk.columns:
                col_data = chunk[column_name]
                if column_name not in unique_values_sets:
                    unique_values_sets[column_name] = set()
                unique_values_sets[column_name].update(col_data.dropna().unique())

                if pd.api.types.is_numeric_dtype(col_data):
                    col_stats = {
                        'column_name': column_name,
                        'mean': col_data.mean(),
                        'median': col_data.median(),
                        'std': col_data.std(),
                        'min': col_data.min(),
                        'max': col_data.max(),
                        '25_percentile': col_data.quantile(0.25),
                        '75_percentile': col_data.quantile(0.75),
                        'unique': col_data.nunique(),
                        'non_null': col_data.count(),
                        'null': col_data.isnull().sum(),
                        'percent_non_null': col_data.count() / total_rows,
                        'percent_unique': col_data.nunique() / total_rows,
                        'mode': col_data.mode().iloc[0] if not col_data.mode().empty else None
                    }
                else:
                    col_stats = {
                        'column_name': column_name,
                        'unique': col_data.nunique(),
                        'non_null': col_data.count(),
                        'null': col_data.isnull().sum(),
                        'mode': col_data.mode().iloc[0] if not col_data.mode().empty else None,
                        'percent_non_null': col_data.count() / total_rows,
                        'percent_unique': col_data.nunique() / total_rows
                    }

                    def stringify_values(value_counts):
                        str_value_counts = {}
                        for k, v in value_counts.items():
                            str_value_counts[str(k)] = v
                        return str_value_counts

                    if show_unique_values:
                        value_counts = col_data.value_counts()
                        str_value_counts = stringify_values(value_counts)

                        if show_unique_counts:
                            col_stats['unique_values'] = str_value_counts
                        else:
                            col_stats['unique_values'] = list(str_value_counts.keys())

                        serialized_unique_values = json.dumps(col_stats['unique_values'], default=str)

                        if max_value_length is not None and len(serialized_unique_values) > max_value_length:
                            if long_value_handling == 'truncate':
                                col_stats['unique_values'] = serialized_unique_values[:max_value_length]
                            elif long_value_handling == 'horizontal':
                                serialized_unique_values_parts = [serialized_unique_values[i:i + max_value_length] for i
                                                                  in
                                                                  range(0, len(serialized_unique_values),
                                                                        max_value_length)]
                                col_stats['unique_values'] = {}
                                for idx, part in enumerate(serialized_unique_values_parts):
                                    new_key = f"unique_values_part[{idx}]"
                                    col_stats['unique_values'][new_key] = part
                            elif long_value_handling == 'explode':
                                serialized_unique_values_parts = [serialized_unique_values[i:i + max_value_length] for i
                                                                  in
                                                                  range(0, len(serialized_unique_values),
                                                                        max_value_length)]
                                col_stats['unique_values'] = []
                                for part in serialized_unique_values_parts:
                                    col_stats['unique_values'].append(part)

                        if uniq_value_mode == 'efficient':
                            if col_stats['percent_non_null'] >= nonnull_threshold:
                                uniq_count_threshold = efficient_mode_multiplier * col_data.nunique()
                                if len(col_stats['unique_values']) > uniq_count_threshold:
                                    col_stats['unique_values'] = 'Exceeded count threshold'

                chunk_stats.append(col_stats)

            result = result.append(chunk_stats, ignore_index=True)
            pbar.update(chunk.shape[0])

    if output_csv is None:
        input_csv_basename = os.path.basename(input_csv)
        filename_without_ext = os.path.splitext(input_csv_basename)[0]
        output_csv = f'col_analysis__{filename_without_ext}.csv'

    result.to_csv(output_csv, index=False, escapechar='"', quoting=csv.QUOTE_ALL)
    return output_csv


def partial_json_loads_ijson(json_string):
    result = {}
    try:
        for prefix, event, value in ijson.parse(StringIO(json_string)):
            if event == 'map_key':
                key = value
            elif event in {'string', 'number', 'boolean', 'null'}:
                result[key] = value
    except ijson.common.IncompleteJSONError:
        pass
    return result


def apply_long_value_handling2(col_stats, max_value_length, long_value_handling):
    serialized_unique_values = json.dumps(col_stats['unique_values'], default=str)

    if max_value_length is not None and len(serialized_unique_values) > max_value_length:
        if long_value_handling == 'truncate':
            col_stats['unique_values'] = serialized_unique_values[:max_value_length]
        elif long_value_handling == 'horizontal':
            serialized_unique_values_parts = [serialized_unique_values[i:i + max_value_length] for i
                                              in range(0, len(serialized_unique_values), max_value_length)]
            col_stats['unique_values'] = {}
            for idx, part in enumerate(serialized_unique_values_parts):
                new_key = f"unique_values_part[{idx}]"
                col_stats['unique_values'][new_key] = part
        elif long_value_handling == 'explode':
            serialized_unique_values_parts = [serialized_unique_values[i:i + max_value_length] for i
                                              in range(0, len(serialized_unique_values), max_value_length)]
            col_stats['unique_values'] = []
            for part in serialized_unique_values_parts:
                col_stats['unique_values'].append(part)

    return col_stats


def apply_long_value_handling3(col_stats, max_value_length, long_value_handling, value_check_mode):
    if value_check_mode is not None:
        serialized_unique_values = json.dumps(col_stats['unique_values'], default=str)

        if value_check_mode == 'both' or value_check_mode == 'values':
            if isinstance(col_stats['unique_values'], dict):
                new_unique_values = {}
                for k, v in col_stats['unique_values'].items():
                    if len(k) > max_value_length:
                        if long_value_handling == 'truncate':
                            new_key = k[:max_value_length]
                        else:
                            new_key = k
                    else:
                        new_key = k
                    new_unique_values[new_key] = v
                col_stats['unique_values'] = new_unique_values

        if max_value_length is not None and len(serialized_unique_values) > max_value_length:
            if long_value_handling == 'truncate':
                col_stats['unique_values'] = serialized_unique_values[:max_value_length]
            elif long_value_handling == 'horizontal':
                serialized_unique_values_parts = [serialized_unique_values[i:i + max_value_length] for i
                                                  in range(0, len(serialized_unique_values), max_value_length)]
                col_stats['unique_values'] = {}
                for idx, part in enumerate(serialized_unique_values_parts):
                    new_key = f"unique_values_part[{idx}]"
                    col_stats['unique_values'][new_key] = part
            elif long_value_handling == 'explode':
                serialized_unique_values_parts = [serialized_unique_values[i:i + max_value_length] for i
                                                  in range(0, len(serialized_unique_values), max_value_length)]
                col_stats['unique_values'] = []
                for part in serialized_unique_values_parts:
                    col_stats['unique_values'].append(part)

    return col_stats


def apply_long_value_handling(unique_values, max_value_length, long_value_handling, value_check_mode):
    if value_check_mode is not None:
        serialized_unique_values = json.dumps(unique_values, default=str)

        if value_check_mode == 'both' or value_check_mode == 'values':
            if isinstance(unique_values, dict):
                new_unique_values = {}
                for k, v in unique_values.items():
                    if len(k) > max_value_length:
                        if long_value_handling == 'truncate':
                            new_key = k[:max_value_length]
                        else:
                            new_key = k
                    else:
                        new_key = k
                    new_unique_values[new_key] = v
                unique_values = new_unique_values

        if max_value_length is not None and len(serialized_unique_values) > max_value_length:
            if long_value_handling == 'truncate':
                unique_values = serialized_unique_values[:max_value_length]
            elif long_value_handling == 'horizontal':
                serialized_unique_values_parts = [serialized_unique_values[i:i + max_value_length] for i
                                                  in range(0, len(serialized_unique_values), max_value_length)]
                unique_values = {}
                for idx, part in enumerate(serialized_unique_values_parts):
                    new_key = f"unique_values_part[{idx}]"
                    unique_values[new_key] = part
            elif long_value_handling == 'explode':
                serialized_unique_values_parts = [serialized_unique_values[i:i + max_value_length] for i
                                                  in range(0, len(serialized_unique_values), max_value_length)]
                unique_values = []
                for part in serialized_unique_values_parts:
                    unique_values.append(part)

    return unique_values


def apply_unique_value_handling(unique_values=None, percent_non_null=None,
                                nonnull_threshold=None, nunique=None,
                                efficient_mode_multiplier=None,
                                uniq_value_mode=None, exceed_value='Exceeded count threshold'):
    if uniq_value_mode == 'efficient':
        # TODO fix issue of never handling efficient case
        #  print(str(percent_non_null) + " :: >= :: " + str(nonnull_threshold))
        if percent_non_null >= nonnull_threshold:
            uniq_count_threshold = efficient_mode_multiplier * nunique
            if len(unique_values) > uniq_count_threshold:
                return exceed_value
    return unique_values


def merge_unique_values(dict1, dict2):
    result = dict1.copy()
    for k, v in dict2.items():
        if k in result:
            result[k] += v
        else:
            result[k] = v
    return result


def generate_column_analytics(input_csv, output_csv=None, chunksize=10000,
                              max_value_length=5000, long_value_handling='truncate',
                              show_unique_values=True, show_unique_counts=True,
                              uniq_value_mode='efficient', nonnull_threshold=0.96,
                              efficient_mode_multiplier=1, value_check_mode='field'
                              , include_chunked_output=False):
    total_rows = count_rows_in_chunks(input_csv, chunksize)

    aggregated_analytics = {}  # Use a dictionary to store aggregated analytics
    unique_values_sets = {}  # Initialize the unique_values_sets dictionary

    if include_chunked_output:  # set variables for case of also outputting chunked data
        # make filename for csv output for chunked data
        input_csv_basename = os.path.basename(input_csv)
        filename_without_ext = os.path.splitext(input_csv_basename)[0]
        output_chunked_csv = f'col_analysis_chunked_{filename_without_ext}.csv'

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:

        # for each chunk of the input csv
        for chunk_num, chunk in enumerate(pd.read_csv(input_csv, chunksize=chunksize, low_memory=False)):
            chunk_stats = []

            def stringify_values(value_counts):
                str_value_counts = {}
                for k, v in value_counts.items():
                    str_value_counts[str(k)] = v
                return str_value_counts

            # for each column_name in each chunk
            for column_name in chunk.columns:
                col_data = chunk[column_name]
                value_counts = col_data.value_counts()
                # Add these lines
                if column_name not in unique_values_sets:
                    unique_values_sets[column_name] = set()
                unique_values_sets[column_name].update(col_data.dropna().unique())

                if pd.api.types.is_bool_dtype(col_data):
                    is_numeric = False
                else:
                    try:
                        pd.to_numeric(col_data)
                        is_numeric = True
                    except (TypeError, ValueError):
                        is_numeric = False

                if is_numeric:
                    col_stats = {
                        'column_name': column_name,
                        'total': total_rows,
                        'mean': np.nanmean(col_data) if col_data.notna().any() else np.nan,
                        'median': np.nanmedian(col_data) if col_data.notna().any() else np.nan,
                        'std': np.nanstd(col_data) if col_data.notna().any() else np.nan,
                        'min': col_data.min(),
                        'max': col_data.max(),
                        '25_percentile': col_data.quantile(0.25),
                        '75_percentile': col_data.quantile(0.75),
                        'unique': len(unique_values_sets[column_name]),
                        'non_null': col_data.count(),
                        'null': col_data.isnull().sum(),
                        'percent_non_null': col_data.count() / total_rows,
                        'percent_unique': col_data.nunique() / total_rows,
                        'mode': col_data.mode().iloc[0] if not col_data.mode().empty else None
                    }

                else:
                    col_stats = {
                        'column_name': column_name,
                        'total': total_rows,
                        'unique': len(unique_values_sets[column_name]),
                        'non_null': col_data.count(),
                        'null': col_data.isnull().sum(),
                        'mode': col_data.mode().iloc[0] if not col_data.mode().empty else None,
                        'percent_non_null': col_data.count() / total_rows,
                        'percent_unique': col_data.nunique() / total_rows
                    }

                if show_unique_values:

                    # get the counts for each unique value for each column_name for each chunk
                    value_counts = col_data.value_counts()
                    str_value_counts = stringify_values(value_counts)

                    if show_unique_counts:
                        col_stats['unique_values'] = str_value_counts
                    else:
                        col_stats['unique_values'] = list(str_value_counts.keys())

                    # apply long value handling on the current chunk (truncate, horizontal, explode)
                    if 'unique_values' in col_stats:
                        col_stats['unique_values'] = apply_long_value_handling(col_stats['unique_values'],
                                                                               max_value_length,
                                                                               long_value_handling,
                                                                               value_check_mode)
                        # apply unique value handling on the current chunk (efficient, None/other)
                        col_stats['unique_values'] = apply_unique_value_handling(col_stats['unique_values'],
                                                                                 col_stats['percent_non_null'],
                                                                                 nonnull_threshold,
                                                                                 col_data.nunique(),
                                                                                 efficient_mode_multiplier,
                                                                                 uniq_value_mode)

                chunk_stats.append(col_stats)

            # created chunked output if need be
            if include_chunked_output:
                # convert chunk_stats list of dicts to a Dataframe
                chunk_stats_df = pd.DataFrame(chunk_stats)

                # Write the DataFrame to a CSV file
                chunk_stats_df.to_csv(output_chunked_csv, index=False, escapechar='"', quoting=csv.QUOTE_ALL, mode='a')

            # aggregate the current chunk_stats into the aggregated_stats
            for col_stats in chunk_stats:
                column_name = col_stats['column_name']

                if column_name not in aggregated_analytics:
                    aggregated_analytics[column_name] = col_stats
                else:
                    agg_stats = aggregated_analytics[column_name]  # updating agg_stats will update the
                    # aggregated_analytics for a specific column_name

                    # Update aggregated values
                    for key, value in col_stats.items():
                        if key in ['unique', 'non_null', 'null']:
                            agg_stats[key] += value
                        elif key in ['mean', 'median', 'std', 'min', 'max', '25_percentile', '75_percentile']:
                            if pd.api.types.is_numeric_dtype(chunk[column_name]) and agg_stats.get(key) is not None:
                                if agg_stats['non_null'] > 0 and col_stats['non_null'] > 0:
                                    agg_stats[key] = (agg_stats.get(key, 0) * agg_stats['non_null'] + value * col_stats[
                                        'non_null']) / (agg_stats['non_null'] + col_stats['non_null'])
                                elif col_stats['non_null'] > 0:
                                    agg_stats[key] = value

                    # Update unique_values dictionary
                    if 'unique_values' in col_stats:
                        if isinstance(col_stats['unique_values'], str):
                            try:
                                col_stats['unique_values'] = json.loads(col_stats['unique_values'])
                            except json.JSONDecodeError:
                                col_stats['unique_values'] = partial_json_loads_ijson(col_stats['unique_values'])

                        if isinstance(agg_stats['unique_values'], str):
                            agg_stats['unique_values'] = partial_json_loads_ijson(agg_stats['unique_values'])

                        if isinstance(col_stats['unique_values'], dict):
                            agg_stats['unique_values'] = agg_stats.get('unique_values', {})
                            '''
                            print(("agg_stats: " +
                                str(agg_stats['unique_values'])[:300] + "...") if len(agg_stats['unique_values']) > 300
                                  else ("agg_stats: " + str(agg_stats['unique_values'])))
                            print(("col_stats: " +
                                str(col_stats['unique_values'])[:300] + "...") if len(col_stats['unique_values']) > 300
                                  else ("col_stats: " + str(col_stats['unique_values'])))
                            '''
                            agg_stats['unique_values'] = merge_unique_values(agg_stats['unique_values'],
                                                                             col_stats['unique_values'])

                    # Recalculate percentages
                    agg_stats['percent_non_null'] = agg_stats['non_null'] / total_rows
                    agg_stats['percent_unique'] = agg_stats['unique'] / total_rows

                    # Apply unique value handling and long_value_handling to the aggregated unique_values dictionary
                    if 'unique_values' in agg_stats and (value_check_mode == 'both' or value_check_mode == 'field'):
                        agg_stats['unique_values'] = apply_unique_value_handling(unique_values=
                                                                                 agg_stats['unique_values'],
                                                                                 percent_non_null=
                                                                                 agg_stats['percent_non_null'],
                                                                                 nonnull_threshold=nonnull_threshold,
                                                                                 nunique=agg_stats['unique'],
                                                                                 efficient_mode_multiplier=
                                                                                 efficient_mode_multiplier,
                                                                                 uniq_value_mode=uniq_value_mode,
                                                                                 exceed_value=
                                                                                 'Exceeded count threshold')

                        serialized_agg_unique_values = json.dumps(agg_stats['unique_values'], default=str)
                        # TODO fix issue with stringifying unique values
                        agg_stats['unique_values'] = apply_long_value_handling(serialized_agg_unique_values,
                                                                               max_value_length, long_value_handling,
                                                                               value_check_mode)
            '''
            column_name_to_track = 'level'
            if column_name_to_track in aggregated_analytics:
                unique_values_str = json.dumps(aggregated_analytics[column_name_to_track]['unique_values'], default=str)
                print(f"After chunk {chunk_num}, unique_values for {column_name_to_track}:")
                print(unique_values_str[:300] + "..." if len(unique_values_str) > 300 else unique_values_str)
            '''

            pbar.update(chunk.shape[0])

    # After processing all chunks, calculate the count of unique values for each column
    for column_name, unique_values_set in unique_values_sets.items():
        aggregated_analytics[column_name]['unique'] = len(unique_values_set)

    # Convert the aggregated analytics dictionary to a DataFrame
    result = pd.DataFrame.from_dict(aggregated_analytics, orient='index').reset_index(drop=True)

    if output_csv is None:
        input_csv_basename = os.path.basename(input_csv)
        filename_without_ext = os.path.splitext(input_csv_basename)[0]
        output_csv = f'col_analysis__{filename_without_ext}.csv'

    result.to_csv(output_csv, index=False, escapechar='"', quoting=csv.QUOTE_ALL)
    return output_csv


def infer_dtypes(file_path, nrows=1000):
    df_sample = pd.read_csv(file_path, nrows=nrows)
    dtypes = df_sample.dtypes.to_dict()
    return {column: str(dtype) for column, dtype in dtypes.items()}


def read_csv_in_chunks_and_infer_dtypes(file_path, chunksize):
    dtypes = infer_dtypes(file_path)
    reader = pd.read_csv(file_path, iterator=True, chunksize=chunksize, dtype=dtypes, low_memory=False)
    for chunk in reader:
        yield chunk


'''
def join_large_csvs(left_file, right_file, left_on, right_on, join_type='inner', chunksize=50000):
    # Get the total number of rows in the left CSV file for progress bar
    total_rows_left = count_rows_in_chunks(left_file, chunksize)

    # Determine the output CSV file name
    input_csv_basename = os.path.basename(left_file)
    filename_without_ext = os.path.splitext(input_csv_basename)[0]
    output_csv = f'joined__{filename_without_ext}.csv'

    # Infer dtypes for left and right CSV files
    left_dtypes = infer_dtypes(left_file)
    right_dtypes = infer_dtypes(right_file)

    # Open the output CSV file
    with open(output_csv, 'w', encoding='utf-8-sig') as f_output:
        writer = None

        # Initialize the progress bar
        with tqdm(total=total_rows_left, desc='Processing chunks', unit='rows', ncols=100) as pbar:
            # Read the right csv file in chunks
            left_chunks = read_csv_in_chunks_and_infer_dtypes(left_file, chunksize)
            for left_chunk in left_chunks:
                # Read the left csv file in chunks
                right_chunks = read_csv_in_chunks_and_infer_dtypes(right_file, chunksize)
                for right_chunk in right_chunks:
                    # Perform the join operation
                    df_chunk = pd.merge(left_chunk, right_chunk, how=join_type, left_on=left_on, right_on=right_on)

                    # Write the chunk to the output CSV file
                    if writer is None:
                        # If this is the first chunk, write the header and the data
                        df_chunk.to_csv(f_output, index=False)
                        writer = True
                    else:
                        # If this is not the first chunk, do not write the header again
                        df_chunk.to_csv(f_output, header=False, mode='a', index=False)

                # Update the progress bar
                pbar.update(chunksize)

    # Return the output CSV file name
    return output_csv
'''


def join_large_csvs(left_file, right_file, left_on, right_on, join_type='left', chunksize=50000, suffixes=('_x', '_y')):
    suffixes = tuple(suffixes)  # Convert list to tuple

    # Get the total number of rows in the left and right CSV files for progress bars
    total_rows_left = count_rows_in_chunks(left_file, chunksize)
    total_rows_right = count_rows_in_chunks(right_file, chunksize)

    # Calculate the total number of comparisons
    total_comparisons = total_rows_left * total_rows_right

    # Determine the output CSV file name
    input_csv_basename = os.path.basename(left_file)
    filename_without_ext = os.path.splitext(input_csv_basename)[0]
    output_csv = f'joined__{filename_without_ext}.csv'

    # Infer dtypes for left and right CSV files
    left_dtypes = infer_dtypes(left_file)
    right_dtypes = infer_dtypes(right_file)

    # Open the output CSV file
    with open(output_csv, 'w', encoding='utf-8-sig') as f_output:
        writer = None

        # Initialize the progress bar for total comparisons
        with CustomComparisonTqdm(total=total_comparisons, desc='Processing comparisons', unit='comparisons',
                                  ncols=100) as pbar:
            # Read the left csv file in chunks
            for left_chunk in read_csv_in_chunks(left_file, chunksize):
                # Read the right csv file in chunks
                for right_chunk in read_csv_in_chunks(right_file, chunksize):
                    # If both suffixes are empty, identify overlapping columns (excluding the merge column)
                    # to be dropped from the right dataframe
                    if suffixes == ('', ''):
                        overlapping_columns = set(left_chunk.columns) & set(right_chunk.columns)
                        if right_on in overlapping_columns:  # Only attempt to remove if it exists
                            overlapping_columns.remove(right_on)  # Ensure the merge column is not dropped
                        right_chunk = right_chunk.drop(columns=overlapping_columns, errors='ignore')

                    df_chunk = pd.merge(left_chunk, right_chunk, how=join_type, left_on=left_on, right_on=right_on,
                                        suffixes=suffixes)

                    # Write the chunk to the output CSV file
                    if writer is None:
                        # If this is the first chunk, write the header and the data
                        df_chunk.to_csv(f_output, index=False)
                        writer = True
                    else:
                        # If this is not the first chunk, do not write the header again
                        df_chunk.to_csv(f_output, header=False, mode='a', index=False)

                    # Update the progress bar based on the right side chunksize
                    pbar.update(chunksize * len(left_chunk))

    # Return the output CSV file name
    return output_csv


def extract_business_units(file_path, column_name='account_name', chunksize=1000):
    pattern = r'\(([^()]+)\)$'
    output_file_name = "withunits__" + os.path.basename(file_path)

    total_rows = count_rows_in_chunks(file_path, chunksize)
    processed_chunks = []

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=False):
            chunk['business_unit'] = chunk[column_name].apply(
                lambda x: re.findall(pattern, x)[-1] if len(re.findall(pattern, x)) >= 1 else '')

            processed_chunks.append(chunk)
            pbar.update(chunk.shape[0])

    df_processed = pd.concat(processed_chunks, ignore_index=True)
    df_processed.to_csv(output_file_name, index=False)

    return output_file_name


def remap_values_in_csv(input_csv, remap_dict, chunksize=1000, create_new_column=True):
    output_file_name = "remapped__" + os.path.basename(input_csv)

    total_rows = count_rows_in_chunks(input_csv, chunksize)
    processed_chunks = []

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(input_csv, chunksize=chunksize):
            for column_name, value_map in remap_dict.items():
                if column_name in chunk.columns:
                    remapped_column = chunk[column_name].map(value_map).fillna(chunk[column_name])
                    if create_new_column:
                        new_column_name = "__" + column_name
                        chunk[new_column_name] = remapped_column
                    else:
                        chunk[column_name] = remapped_column

            processed_chunks.append(chunk)
            pbar.update(chunk.shape[0])

    df_processed = pd.concat(processed_chunks, ignore_index=True)
    df_processed.to_csv(output_file_name, index=False)

    return output_file_name


def rename_csv_file(input_csv, new_name):
    # Get the directory of the input CSV file
    input_dir = os.path.dirname(input_csv)

    # Get the extension of the input CSV file
    input_ext = os.path.splitext(input_csv)[-1]

    # Combine the directory, new name, and extension to create the new file path
    new_file_path = os.path.join(input_dir, f"{new_name}{input_ext}")

    # Rename the CSV file by moving it to the new file path
    shutil.move(input_csv, new_file_path)


'''
def join_large_csvs(left_file, right_file, left_on, right_on, join_type='inner', chunksize=50000):
    # Get the total number of rows in the left CSV file for progress bar
    total_rows_left = count_rows_in_chunks(left_file, chunksize)

    # Determine the output CSV file name
    input_csv_basename = os.path.basename(left_file)
    filename_without_ext = os.path.splitext(input_csv_basename)[0]
    output_csv = f'joined__{filename_without_ext}.csv'

    # Infer dtypes for left and right CSV files
    left_dtypes = infer_dtypes(left_file)
    right_dtypes = infer_dtypes(right_file)

    # Open the output CSV file
    with open(output_csv, 'w', encoding='utf-8-sig') as f_output:
        writer = None

        # Initialize the progress bar
        with tqdm(total=total_rows_left, desc='Processing chunks', unit='rows', ncols=100) as pbar:
            # Read the right csv file in chunks
            left_chunks = read_csv_in_chunks_and_infer_dtypes(left_file, chunksize)
            for left_chunk in left_chunks:
                # Read the left csv file in chunks
                right_chunks = read_csv_in_chunks_and_infer_dtypes(right_file, chunksize)
                for right_chunk in right_chunks:
                    for left_index, left_row in left_chunk.iterrows():
                        for right_index, right_row in right_chunk.iterrows():
                            # Perform the join operation
                            if left_row[left_on] == right_row[right_on]:
                                df_chunk = pd.concat([left_row, right_row], axis=1)

                                # Write the chunk to the output CSV file
                                if writer is None:
                                    # If this is the first chunk, write the header and the data
                                    df_chunk.to_csv(f_output, index=False)
                                    writer = True
                                else:
                                    # If this is not the first chunk, do not write the header again
                                    df_chunk.to_csv(f_output, header=False, mode='a', index=False)

                # Update the progress bar
                pbar.update(chunksize)

    # Return the output CSV file name
    return output_csv
'''


def is_ip_address(value: Union[str, List[str]]) -> bool:
    ip_regex = r'\b(?:(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|(?:(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}|(?:[0-9A-Fa-f]{1,4}:){1,7}:|(?:[0-9A-Fa-f]{1,4}:){1,6}:[0-9A-Fa-f]{1,4}|(?:[0-9A-Fa-f]{1,4}:){1,5}(?::[0-9A-Fa-f]{1,4}){1,2}|(?:[0-9A-Fa-f]{1,4}:){1,4}(?::[0-9A-Fa-f]{1,4}){1,3}|(?:[0-9A-Fa-f]{1,4}:){1,3}(?::[0-9A-Fa-f]{1,4}){1,4}|(?:[0-9A-Fa-f]{1,4}:){1,2}(?::[0-9A-Fa-f]{1,4}){1,5}|[0-9A-Fa-f]{1,4}:(?::[0-9A-Fa-f]{1,4}){1,6}|:(?::[0-9A-Fa-f]{1,4}){1,7}|::))\b'
    if isinstance(value, str):
        return bool(re.search(ip_regex, value))
    elif isinstance(value, list):
        return any(re.search(ip_regex, str(item)) for item in value)
    return False


# TODO function like this that can run over sets of objects /(large JSON files)
def find_ip_keys(data: Union[Dict, List], key_path: str, target_keys: List[str], threshold: float, result: List[str],
                 use_jaccard: bool):
    if isinstance(data, dict):
        for key, value in data.items():
            new_key_path = f"{key_path}.{key}" if key_path else key
            if use_jaccard and any(jaccard_index(key, target) >= threshold for target in target_keys):
                if is_ip_address(value):
                    result.append(new_key_path)
            elif not use_jaccard and is_ip_address(value):
                result.append(new_key_path)
            find_ip_keys(value, new_key_path, target_keys, threshold, result, use_jaccard)
    elif isinstance(data, list):
        for index, item in enumerate(data):
            find_ip_keys(item, f"{key_path}[{index}]", target_keys, threshold, result, use_jaccard)


def find_ip_keys_in_json(json_file: str, target_keys: List[str], threshold: float, use_jaccard: bool) -> List[str]:
    with open(json_file, 'r') as file:
        data = json.load(file)
    result = []
    find_ip_keys(data, "", target_keys, threshold, result, use_jaccard)
    return result


def array_to_csv(array: List[str], column_name: str, file_name: str):
    with open(file_name + '.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile, dialect='excel')
        csvwriter.writerow([column_name])
        for value in array:
            csvwriter.writerow([value])


def extract_first_value_from_lists_in_csv2(
        file_path: str,
        columns: List[str],
        replace_old_column: bool = False,
        chunksize: int = 10000
):
    # Count the rows
    total_rows = count_rows(file_path)

    # Read the CSV in chunks and process it
    reader = pd.read_csv(file_path, iterator=True, chunksize=chunksize)

    # Create an empty dataframe to store the processed chunks
    processed_df = pd.DataFrame()

    with tqdm(total=total_rows, ncols=100, desc="Processing rows") as pbar:
        for chunk in reader:
            for col in columns:
                if col in chunk.columns:
                    # Extract the first value from the lists in the specified column
                    chunk[col] = chunk[col].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)

                    # Create a new column if replace_old_column is False
                    if not replace_old_column:
                        new_col_name = "__" + col
                        chunk[new_col_name] = chunk[col]
                        chunk = chunk.drop(columns=[col])

            # Append the processed chunk to the processed dataframe
            processed_df = processed_df.append(chunk, ignore_index=True)

            # Update the progress bar
            pbar.update(chunk.shape[0])

    # Save the processed dataframe to a new CSV file
    processed_df.to_csv("list_extr__" + file_path, index=False)
    return "processed_" + file_path


def extract_first_value_from_lists_in_csv(file_path, columns_to_extract, replace_old_column=False, chunksize=1000):
    # Get the total number of rows in the CSV file using chunks
    total_rows = count_rows_in_chunks(file_path, chunksize)

    new_file_path = "list_extr__" + file_path
    first_chunk = True

    def extract_first_value(value):
        try:
            value_list = ast.literal_eval(value)
            if isinstance(value_list, list) and len(value_list) > 0:
                return value_list[0]
        except (SyntaxError, ValueError):
            pass
        return value

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=False, iterator=True):
            for col in columns_to_extract:
                new_col = "__" + col if not replace_old_column else col

                chunk[new_col] = chunk[col].apply(extract_first_value)
                if replace_old_column:
                    chunk.drop(col, axis=1, inplace=True)

            if first_chunk:
                chunk.to_csv(new_file_path, index=False)
                first_chunk = False
            else:
                chunk.to_csv(new_file_path, mode='a', header=False, index=False)

            pbar.update(chunk.shape[0])

    return new_file_path


def select_columns_from_csv(csv_filepath, column_names, chunksize=10000):
    # Get the total number of rows in the CSV file using chunks
    total_rows = count_rows_in_chunks(csv_filepath, chunksize)

    # Create the output CSV file path with "col_clip__" appended to the front
    output_csv_filepath = os.path.join(os.path.dirname(csv_filepath),
                                       f"col_clip__{os.path.basename(csv_filepath)}")
    print(output_csv_filepath)

    # Read the CSV file in chunks, select the specified columns, and write to the output CSV file
    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        header_written = False
        for chunk in pd.read_csv(csv_filepath, chunksize=chunksize, low_memory=False, iterator=True):
            # Select the specified columns
            selected_chunk = chunk[column_names]

            # Write the chunk to the output CSV file
            if not header_written:
                selected_chunk.to_csv(output_csv_filepath, index=False)
                header_written = True
            else:
                selected_chunk.to_csv(output_csv_filepath, index=False, header=False, mode='a')

            pbar.update(chunk.shape[0])
    return output_csv_filepath


def fill_empty_values_in_csv(csv_filepath, fill_values_dict, chunksize=10000):
    total_rows = count_rows_in_chunks(csv_filepath, chunksize)

    output_csv_filepath = os.path.join(os.path.dirname(csv_filepath),
                                       f"filled__{os.path.basename(csv_filepath)}")

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        header_written = False
        for chunk in pd.read_csv(csv_filepath, chunksize=chunksize, low_memory=False):
            # Fill empty values in specified columns with values from the dictionary
            for column, fill_value in fill_values_dict.items():
                chunk[column].fillna(fill_value, inplace=True)

            # Write the updated chunk to the output CSV file
            if not header_written:
                chunk.to_csv(output_csv_filepath, index=False)
                header_written = True
            else:
                chunk.to_csv(output_csv_filepath, index=False, header=False, mode='a')

            pbar.update(chunk.shape[0])


def remove_rows_with_empty_values(csv_filepath, columns_to_check, chunksize=10000):
    total_rows = count_rows_in_chunks(csv_filepath, chunksize)

    output_csv_filepath = os.path.join(os.path.dirname(csv_filepath),
                                       f"no_empty__{os.path.basename(csv_filepath)}")

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        header_written = False
        for chunk in pd.read_csv(csv_filepath, chunksize=chunksize, low_memory=False):
            # Remove rows with empty values in specified columns
            chunk.dropna(subset=columns_to_check, inplace=True)

            # Write the updated chunk to the output CSV file
            if not header_written:
                chunk.to_csv(output_csv_filepath, index=False)
                header_written = True
            else:
                chunk.to_csv(output_csv_filepath, index=False, header=False, mode='a')

            pbar.update(chunk.shape[0])


def format_datetime_columns_in_csv(csv_filepath, datetime_columns, datetime_format='%Y-%m-%d', chunksize=10000):
    total_rows = count_rows_in_chunks(csv_filepath, chunksize)

    output_csv_filepath = os.path.join(os.path.dirname(csv_filepath),
                                       f"formatted_datetime__{os.path.basename(csv_filepath)}")

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        header_written = False
        for chunk in pd.read_csv(csv_filepath, chunksize=chunksize, low_memory=False):
            # Format datetime columns
            for column in datetime_columns:
                chunk[column] = pd.to_datetime(chunk[column]).dt.strftime(datetime_format)

            # Write the updated chunk to the output CSV file
            if not header_written:
                chunk.to_csv(output_csv_filepath, index=False)
                header_written = True
            else:
                chunk.to_csv(output_csv_filepath, index=False, header=False, mode='a')

            pbar.update(chunk.shape[0])


def transform_columns_in_csv_old(input_csv, transformations_dict, chunksize=10000):
    total_rows = count_rows_in_chunks(input_csv, chunksize)

    # Convert strings to lambda functions
    for column, transformation in transformations_dict.items():
        if isinstance(transformation, str):
            transformations_dict[column] = eval('lambda row: ' + transformation)

    output_csv_filepath = os.path.join(os.path.dirname(input_csv),
                                       f"transformed__{os.path.basename(input_csv)}")

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            for column, transformation in transformations_dict.items():
                if column in chunk.columns:
                    chunk[column] = chunk.apply(transformation, axis=1)
                else:
                    chunk[column] = chunk.apply(transformation, axis=1)
            if os.path.exists(output_csv_filepath):
                chunk.to_csv(output_csv_filepath, mode='a', header=False, index=False)
            else:
                chunk.to_csv(output_csv_filepath, index=False)
            pbar.update(chunk.shape[0])


def transform_columns_in_csv(input_csv, transformations_dict, chunksize=10000):
    total_rows = count_rows_in_chunks(input_csv, chunksize)

    # Convert strings to lambda functions
    for column, transformation in transformations_dict.items():
        if isinstance(transformation, str):
            transformations_dict[column] = eval('lambda row: ' + transformation)

    output_csv_filepath = os.path.join(os.path.dirname(input_csv),
                                       f"transformed__{os.path.basename(input_csv)}")

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        fieldnames = set()
        with open(output_csv_filepath, 'w+', newline='', encoding='utf-8') as outfile:
            writer = DynamicDictWriter(outfile, fieldnames=fieldnames, dialect='excel')
            for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
                rows = []
                for _, row in chunk.iterrows():
                    new_row = row.to_dict()
                    for column, transformation in transformations_dict.items():
                        new_row[column] = transformation(new_row)
                    rows.append(new_row)
                transformed_chunk = pd.DataFrame(rows)
                for row in transformed_chunk.to_dict('records'):
                    writer.writerow(row)
                pbar.update(transformed_chunk.shape[0])

    return output_csv_filepath


def bulk_value_search(csv_filepath, values_to_search, truncation_limit = 32000, chunksize = 10000):
    # Count the total number of rows
    total_rows = count_rows_in_chunks(csv_filepath, chunksize)

    # Create the output CSV file path with "bulk_search_results__" appended to the front
    output_csv_filepath = os.path.join(os.path.dirname(csv_filepath),
                                       f"bulk_search_results__{os.path.basename(csv_filepath)}")
    print(output_csv_filepath)

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        header_written = False
        for chunk in pd.read_csv(csv_filepath, chunksize=chunksize, low_memory=False, iterator=True):
            results = []
            for col in chunk.columns:
                for value in values_to_search:
                    if isinstance(value, list):
                        column_values = chunk[col].dropna().unique()
                        column_values = [str(val) for val in column_values]
                        if jaccard_index(column_values, value) > 0:
                            unique_counts = chunk[col].value_counts().head(truncation_limit).to_dict()
                            results.append(
                                {'Column': col, 'Search Value': value, 'Unique Matched Values': unique_counts})
                    else:
                        if chunk[col].isin([value]).any():
                            unique_counts = chunk[col].value_counts().head(truncation_limit).to_dict()
                            results.append(
                                {'Column': col, 'Search Value': value, 'Unique Matched Values': unique_counts})

            result_df = pd.DataFrame(results)

            # Write the results to the output CSV file
            if not header_written:
                result_df.to_csv(output_csv_filepath, index=False)
                header_written = True
            else:
                result_df.to_csv(output_csv_filepath, index=False, header=False, mode='a')

            pbar.update(chunk.shape[0])


def generate_pivot_table2(input_csv, index_cols, pivot_cols=None, value_cols=None, aggfunc='count', chunksize=10000):
    # Handle empty strings for pivot_cols and value_cols
    pivot_cols = None if not pivot_cols else pivot_cols
    value_cols = None if not value_cols else value_cols

    # Create the output CSV file path with "pivot__" appended to the front
    output_csv_filepath = os.path.join(os.path.dirname(input_csv), f"pivot__{os.path.basename(input_csv)}")

    # Count the total number of rows
    total_rows = count_rows_in_chunks(input_csv, chunksize)

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        # Initialize DataFrame for the full pivot table
        full_pivot_table = pd.DataFrame()

        # Iterate over the input CSV file in chunks
        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            # Generate the pivot table for the current chunk
            chunk_pivot_table = chunk.pivot_table(index=index_cols, columns=pivot_cols, values=value_cols,
                                                  aggfunc=aggfunc)

            # Concatenate the chunk's pivot table to the full pivot table
            full_pivot_table = pd.concat([full_pivot_table, chunk_pivot_table])

            pbar.update(chunk.shape[0])

    # Write the full pivot table to the output CSV file
    full_pivot_table.to_csv(output_csv_filepath)

    return output_csv_filepath


def generate_pivot_table3(input_csv, index_cols, pivot_cols=None, value_cols=None, aggfunc='count', chunksize=10000):
    # Create the output CSV file path with "pivot__" appended to the front
    output_csv_filepath = os.path.join(os.path.dirname(input_csv), f"pivot__{os.path.basename(input_csv)}")

    # Count the total number of rows
    total_rows = count_rows_in_chunks(input_csv, chunksize)

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        # Initialize DataFrame for the full pivot table
        full_pivot_table = pd.DataFrame()
        total_pivot_table = pd.DataFrame()

        # Iterate over the input CSV file in chunks
        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            # Generate the pivot table for the current chunk
            chunk_pivot_table = chunk.pivot_table(index=index_cols, columns=pivot_cols, values=value_cols,
                                                  aggfunc=aggfunc)

            # Generate the pivot table for total counts
            chunk_total_pivot_table = chunk.pivot_table(index=index_cols, columns=pivot_cols, values=value_cols,
                                                        aggfunc='size')

            # Concatenate the chunk's pivot table to the full pivot table
            full_pivot_table = pd.concat([full_pivot_table, chunk_pivot_table])

            # Concatenate the chunk's total pivot table to the total pivot table
            total_pivot_table = pd.concat([total_pivot_table, chunk_total_pivot_table])

            pbar.update(chunk.shape[0])

    # Add a column for totals
    full_pivot_table['Total'] = total_pivot_table.sum(axis=1)

    # Write the full pivot table to the output CSV file
    full_pivot_table.to_csv(output_csv_filepath)

    return output_csv_filepath


def generate_pivot_table213(input_csv, index_cols, pivot_cols=None, value_cols=None, aggfunc='count', chunksize=10000):
    pivot_cols = None if not pivot_cols else pivot_cols
    value_cols = None if not value_cols else value_cols

    output_csv_filepath = os.path.join(os.path.dirname(input_csv), f"pivot__{os.path.basename(input_csv)}")

    total_rows = count_rows_in_chunks(input_csv, chunksize)

    total_count = 0
    unique_values = set()

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        full_pivot_table = pd.DataFrame()

        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            total_count += chunk[value_cols].count().sum()
            unique_values.update(chunk[value_cols].unique())

            if aggfunc == 'unique count':
                chunk_pivot_table = chunk.pivot_table(index=index_cols, columns=pivot_cols, values=value_cols,
                                                      aggfunc=pd.Series.nunique)
            else:
                chunk_pivot_table = chunk.pivot_table(index=index_cols, columns=pivot_cols, values=value_cols,
                                                      aggfunc=aggfunc)
            full_pivot_table = pd.concat([full_pivot_table, chunk_pivot_table])

            pbar.update(chunk.shape[0])

    unique_count = len(unique_values)
    full_pivot_table['Total Count'] = total_count
    full_pivot_table['Unique Count'] = unique_count

    full_pivot_table.to_csv(output_csv_filepath)

    return output_csv_filepath


def generate_pivot_table2(input_csv, index_cols, pivot_cols=None, value_cols=None, aggfunc='count', chunksize=10000):
    pivot_cols = None if not pivot_cols else pivot_cols
    value_cols = None if not value_cols else value_cols

    output_csv_filepath = os.path.join(os.path.dirname(input_csv), f"pivot__{os.path.basename(input_csv)}")

    total_rows = count_rows_in_chunks(input_csv, chunksize)

    total_count = 0
    unique_values = set()

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        full_pivot_table = pd.DataFrame()

        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            total_count += chunk[value_cols[0]].count().sum()
            for col in value_cols:
                unique_values.update(chunk[col].unique())

            if aggfunc == 'unique count':
                chunk_pivot_table = chunk.pivot_table(index=index_cols, columns=pivot_cols, values=value_cols[0],
                                                      aggfunc=pd.Series.nunique)
            else:
                chunk_pivot_table = chunk.pivot_table(index=index_cols, columns=pivot_cols, values=value_cols[0],
                                                      aggfunc=aggfunc)
            full_pivot_table = pd.concat([full_pivot_table, chunk_pivot_table])

            pbar.update(chunk.shape[0])

    unique_count = len(unique_values)
    full_pivot_table['Total Count'] = total_count
    full_pivot_table['Unique Count'] = unique_count

    full_pivot_table.to_csv(output_csv_filepath)

    return output_csv_filepath


def generate_pivot_table3(input_csv, index_cols, pivot_cols=None, value_cols=None, aggfunc='count', chunksize=10000):
    pivot_cols = None if not pivot_cols else pivot_cols
    value_cols = None if not value_cols else value_cols

    output_csv_filepath = os.path.join(os.path.dirname(input_csv), f"pivot__{os.path.basename(input_csv)}")

    total_rows = count_rows_in_chunks(input_csv, chunksize)

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        # Initialize a list of DataFrames for each value column and aggregation function
        full_pivot_tables = []

        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            for col in value_cols:
                # Create a pivot table for each aggregation function
                for func in [pd.Series.nunique, 'count']:
                    chunk_pivot_table = chunk.pivot_table(index=index_cols, columns=pivot_cols, values=col,
                                                          aggfunc=func)
                    full_pivot_tables.append(chunk_pivot_table)

            pbar.update(chunk.shape[0])

    # Concatenate all pivot tables along the columns axis
    full_pivot_table = pd.concat(full_pivot_tables, axis=1)

    full_pivot_table.to_csv(output_csv_filepath)

    return output_csv_filepath


def generate_pivot_table(input_csv, index_cols, pivot_cols=None, value_cols=None, chunksize=10000):
    pivot_cols = None if not pivot_cols else pivot_cols
    value_cols = None if not value_cols else value_cols

    output_csv_filepath = os.path.join(os.path.dirname(input_csv), f"pivot__{os.path.basename(input_csv)}")

    total_rows = count_rows_in_chunks(input_csv, chunksize)

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        full_data = pd.DataFrame()

        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            full_data = pd.concat([full_data, chunk])
            pbar.update(chunk.shape[0])

    full_pivot_tables = []

    for col in value_cols:
        # Create a pivot table with total counts
        full_pivot_table_count = full_data.pivot_table(index=index_cols, columns=pivot_cols, values=col,
                                                       aggfunc='count', fill_value=0)
        full_pivot_table_count.columns = [f'{col}_count' for _ in full_pivot_table_count.columns]

        # Create a pivot table with unique counts
        full_pivot_table_nunique = full_data.pivot_table(index=index_cols, columns=pivot_cols, values=col,
                                                         aggfunc=pd.Series.nunique, fill_value=0)
        full_pivot_table_nunique.columns = [f'{col}_nunique' for _ in full_pivot_table_nunique.columns]

        full_pivot_tables.extend([full_pivot_table_count, full_pivot_table_nunique])

    # Concatenate all pivot tables along the columns axis
    full_pivot_table = pd.concat(full_pivot_tables, axis=1)

    full_pivot_table.to_csv(output_csv_filepath)

    return output_csv_filepath

