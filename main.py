import csv

from searchAndFlatten import search_and_flatten_to_csv, get_flattened_csv_headers_from_json
from buildJsonExample import build_example_json
from utils import trim_json, bulk_rename_csv_headers, reformat_json, truncate_json, collapse_json, \
    filter_rows_by_priority, unique_values_with_counts_chunked, generate_column_analytics, \
    join_large_csvs, extract_business_units, remap_values_in_csv, rename_csv_file, find_ip_keys_in_json, \
    array_to_csv, extract_first_value_from_lists_in_csv, select_columns_from_csv, fill_empty_values_in_csv, \
    remove_rows_with_empty_values, format_datetime_columns_in_csv, transform_columns_in_csv, \
    bulk_value_search, generate_pivot_table, process_csv_remove_parentheses, csv_analytics

import argparse
import json
from json.decoder import JSONDecodeError
from colorama import Fore, Style, init


def load_json_file(file_path):
    try:
        with open(file_path, encoding='utf-8') as file:
            return json.load(file)
    except JSONDecodeError as e:
        print(f"{Fore.RED}[!] Error parsing the config file '{file_path}':")
        print(f"    - Line {e.lineno}, Column {e.colno}")
        print(f"    - {e.msg}{Style.RESET_ALL}")
        return None


def print_job_start(job_index, total_jobs, job_name, job):
    print(f"\nJob {job_index + 1}/{total_jobs} -[{job_name}] started.  Type ::{job.get('type')}::")


# Uses config.json file to detect jobs of certain types and then runs them
def main():
    # Initialize colorama
    init()

    parser = argparse.ArgumentParser(description='Process config file for jobs.')
    parser.add_argument('-c', '--config', metavar='config_file', default='config.json',
                        help='Path to the config file (default: config.json)')

    args = parser.parse_args()

    print(f'[+] Loading config file - {args.config}')
    config = load_json_file(args.config)

    if config is None:
        print(f'[-] Exiting due to JSON parsing error.')
        return

    jobs = config.get("jobs", [])

    job_vars = {
        'last_file': '',
        'output_files': []
    }

    total_jobs = len(jobs)
    for job_index, job in enumerate(jobs):
        job_name = job.get("name")

        job_matched = False

        # search and flatten to csv job
        if job.get("type") == "search_and_flatten_csv":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            search_config_path = job.get("search_config_path")  # get search config path job variable
            search_configs = job.get("searchconfigs") or []  # get the search configs job variable
            delimiter = job.get("delimiter") or ","  # obtain the delimiter job variable

            with open(search_config_path) as search_config_file:
                try:
                    all_search_configs = json.load(search_config_file)
                except Exception as e:
                    # print the error message in red
                    print("\033[91mYour searches.json file is not formatted correctly or messed up:", e, "\033[0m")

            for search_index, (search_name, file_name) in enumerate(search_configs.items()):
                current_config = all_search_configs.get(search_name)
                if not current_config:
                    continue
                options = {
                    "input_json": file_name,
                    "root_key": current_config.get("root_key", None),
                    "similarity_threshold": current_config.get("similarity_threshold", 1.0),
                    "search_config": current_config.get("search_config", "*"),
                    "array_handling": current_config.get("array_handling", "stringify"),
                    "object_handling": current_config.get("object_handling", "stringify"),
                    "allow_dot_notation": current_config.get("allow_dot_notation", False),
                    "delimiter": delimiter,
                    "search_name": search_name,
                    "verbose": job.get("verbose", False),
                    "separator": ".",
                    "mode": job.get("mode", "normal"),
                    "num_test_rows": job.get("num_test_rows", None),
                    "max_string_length": current_config.get("max_string_length", 32759),
                    "long_string_handling": current_config.get("long_string_handling", "truncate"),  # truncate,
                    # explode,
                    # horizontal
                    "quote_handling": current_config.get("quote_handling", None),  # None, escape, double
                    "quote_values": current_config.get("quote_values", False),  # True, False
                    "quoting": current_config.get("quoting", csv.QUOTE_ALL),  # csv.QUOTE_MINIMAL,
                    # csv.QUOTE_ALL,
                    # csv.QUOTE_NONNUMERIC,
                    # csv.QUOTE_NONE
                    "escapechar": current_config.get("escapechar", None),  # '\\' or None
                    "remove_quotes": current_config.get("remove_quotes", True)
                }
                print(f'{job_index + 1}.{search_index + 1}) SearchAndFlattenCSV, name:"{search_name}", '
                      f'#ofsearches:{len(search_configs)}')
                output_csv_filename = search_and_flatten_to_csv(**options)
                job_vars['last_file'] = output_csv_filename
                job_vars['output_files'].append(output_csv_filename)

        # bulk rename csv columns job
        if job.get("type") == "rename_columns":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            sim_thresh = job.get("similarity_threshold", None)
            input_csv = job.get("input_csv")
            rename_obj = job.get("rename_obj")
            csv_output = bulk_rename_csv_headers(input_csv=input_csv,
                                                 rename_obj=rename_obj,
                                                 threshold=sim_thresh)
            print(f'[+] "bulk_rename_csv", output: {csv_output}')

        # reformat JSON file
        if job.get("type") == "reformat_json":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_json = job.get("input_json", None)
            print(f'[+] Reformatting JSON - {input_json}')
            output_json = reformat_json(input_json=input_json)
            print(f'[+] "reformat_json", output: {output_json}')

        # download JSON from API TODO NOT STARTED YET
        if job.get("type") == "get_from_API":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_json = job.get("input_json", None)
            print(f'[+] Reformatting JSON - {input_json}')
            output_json = reformat_json(input_json=input_json)
            print(f'[+] "reformat_json", output: {output_json}')

        # build example JSON from large JSON (shows as potential keys/fields)
        if job.get("type") == "build_json_example":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            root_key = job.get("root_key")
            input_json = job.get("input_json")
            ignore_new_array_indices = job.get("ignore_new_array_indices")
            json_output = build_example_json(root_key=root_key, input_json=input_json,
                                             ignore_new_array_indices=ignore_new_array_indices)
            print(f'[+] "build_json_example", output: {json_output}')

        if job.get("type") == "analyze_outputs":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            generate_output = job.get("generate_output")
            for file in job_vars['output_files']:
                output = csv_analytics(input_csv=file)
                print(f'[+] "analyze_outputs", output: {output}')

        if job.get("type") == "trim_json":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            range_str = job.get("range")
            input_json = job.get("input_json")
            root_key = job.get("root_key")
            output = trim_json(input_json=input_json, root_key=root_key, range_str=range_str)
            print(f'[+] "trim_json", output: {output}')

        if job.get("type") == "truncate_json":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            depth = job.get("depth", 1)
            input_json = job.get("input_json")
            root_key = job.get("root_key")
            output = truncate_json(input_json=input_json, root_key=root_key, depth=depth)
            print(f'[+] "truncate_json", output: {output}')

        if job.get("type") == "collapse_json":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            depth = job.get("depth", 1)
            input_json = job.get("input_json")
            root_key = job.get("root_key")
            output = collapse_json(input_json=input_json, root_key=root_key, depth=depth)
            print(f'[+] "collapse_json", output: {output}')

        if job.get("type") == "get_flattened_headers":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_json = job.get("input_json")
            root_key = job.get("root_key", None)
            num_test_rows = job.get("num_test_rows", None)
            separator = job.get("separator", ".")
            mode = job.get("mode", "normal")
            # TODO delimiter = job.get("delimiter")

            output = get_flattened_csv_headers_from_json(input_json=input_json, root_key=root_key, mode=mode,
                                                         num_test_rows=num_test_rows, separator=separator)
            print(f'[+] "get_flattened_headers", output: {output}')

        if job.get("type") == "get_unique_values":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            column_names = job.get("column_names")
            output = unique_values_with_counts_chunked(input_csv=input_csv,
                                                       column_names=column_names)
            print(f'[+] "get_unique_values", output: {output}')

        if job.get("type") == "get_column_analytics":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            output = generate_column_analytics(input_csv=input_csv,
                                               show_unique_counts=True,
                                               show_unique_values=True,
                                               efficient_mode_multiplier=5,
                                               uniq_value_mode='normal',
                                               nonnull_threshold=0.96,
                                               long_value_handling='truncate',
                                               max_value_length=32750,
                                               include_chunked_output=False,
                                               value_check_mode='field')
            print(f'[+] "get_column_analytics", output: {output}')

        # TODO finish this or refactor
        if job.get("type") == "analyze_csv":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            output = csv_analytics(input_csv=input_csv)
            print(f'[+] "analyze_csv", output: {output}')

        if job.get("type") == "custom_filter":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            row_limit = job.get("row_limit")
            filter_config = job.get("filter_config")
            score_breakdown = job.get("score_breakdown", False)
            drop_score = job.get("drop_score")
            drop_below = job.get("drop_below", None)
            output = filter_rows_by_priority(input_csv=input_csv,
                                             row_limit=row_limit,
                                             filter_config=filter_config,
                                             drop_score=drop_score,
                                             score_breakdown=score_breakdown,
                                             drop_below=drop_below)
            print(f'[+] "custom_filter", output: {output}')

        if job.get("type") == "join_csvs":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            left_csv = job.get("left_csv")
            right_csv = job.get("right_csv")
            left_on = job.get("left_on")
            right_on = job.get("right_on")
            join_type = job.get("join_type", "left")
            suffixes = job.get("suffixes", ['_x', '_y'])
            chunksize = job.get("chunksize", 10000)
            output = join_large_csvs(left_file=left_csv,
                                     right_file=right_csv,
                                     left_on=left_on,
                                     right_on=right_on,
                                     join_type=join_type,
                                     chunksize=chunksize,
                                     suffixes=suffixes)
            print(f'[+] "join_csvs", output: {output}')

        if job.get("type") == "extract_business_units":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            chunksize = job.get("chunksize", 10000)
            output = extract_business_units(file_path=input_csv,
                                            column_name='account_name',
                                            chunksize=chunksize)
            print(f'[+] "extract_business_units", output: {output}')

        if job.get("type") == "remap_values_in_csv":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            remap_dict = job.get("remap_dict")
            chunksize = job.get("chunksize", 10000)
            output = remap_values_in_csv(input_csv=input_csv,
                                         remap_dict=remap_dict,
                                         chunksize=chunksize,
                                         create_new_column=True)
            print(f'[+] "remap_values_in_csv", output: {output}')

        if job.get("type") == "rename_csv":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            output_name = job.get("output_name")
            output = rename_csv_file(input_csv=input_csv,
                                     new_name=output_name)
            print(f'[+] "rename_csv", output: {output}')

        if job.get("type") == "get_ip_keys":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_json = job.get("input_json")
            keys_array = find_ip_keys_in_json(json_file=input_json,
                                              target_keys=["ip", "ipAddress", "host", "server"],
                                              threshold=0.9,
                                              use_jaccard=False)
            print(keys_array)
            array_to_csv(keys_array, 'Potential_IP_keys', 'potential_JSON_IP_keys')
            print(f'[+] "get_ip_keys", output: potential_JSON_IP_keys.json')

        if job.get("type") == "extract_first_value_from_lists":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            replace_old_column = job.get("replace_old_column", False)
            columns = job.get("columns")
            output = extract_first_value_from_lists_in_csv(file_path=input_csv,
                                                           columns_to_extract=columns,
                                                           replace_old_column=replace_old_column,
                                                           )
            print(f'[+] "extract_first_value_from_lists", output: {output}')

        if job.get("type") == "select_columns_from_csv":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            columns = job.get("columns")
            output = select_columns_from_csv(csv_filepath=input_csv,
                                             column_names=columns)
            print(f'[+] "select_columns_from_csv", output: {output}')

        if job.get("type") == "fill_empty_values_in_csv":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            fill_values_dict = job.get("fill_values_dict")
            output = fill_empty_values_in_csv(csv_filepath=input_csv,
                                              fill_values_dict=fill_values_dict)
            print(f'[+] "fill_empty_values_in_csv", output: {output}')

        if job.get("type") == "remove_rows_with_empty_values":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            columns = job.get("columns")
            output = remove_rows_with_empty_values(csv_filepath=input_csv,
                                                   columns_to_check=columns)
            print(f'[+] "remove_rows_with_empty_values", output: {output}')

        if job.get("type") == "format_datetime_columns_in_csv":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            columns = job.get("columns")
            output = format_datetime_columns_in_csv(csv_filepath=input_csv,
                                                    datetime_columns=columns)
            print(f'[+] "format_datetime_columns_in_csv", output: {output}')

        if job.get("type") == "transform_columns_in_csv":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            transformations = job.get("transformations")
            output = transform_columns_in_csv(input_csv=input_csv,
                                              transformations_dict=transformations)
            print(f'[+] "transform_columns_in_csv", output: {output}')

        if job.get("type") == "bulk_value_search":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            search = job.get("search")
            value_limit = job.get("value_limit")
            output = bulk_value_search(csv_filepath=input_csv,
                                       values_to_search=search,
                                       truncation_limit=value_limit)
            print(f'[+] "bulk_value_search", output: {output}')

        if job.get("type") == "pivot_table":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            index_cols = job.get("index_cols")
            pivot_cols = job.get("pivot_cols")
            value_cols = job.get("value_cols")
            group_by_cols = job.get("group_by_cols")
            group_by = job.get("group_by")
            aggfunc = job.get("aggfunc") # TODO: revisit pivot table later
            output = generate_pivot_table(input_csv=input_csv,
                                          index_cols=index_cols,
                                          pivot_cols=pivot_cols,
                                          value_cols=value_cols,
                                          aggfunc=aggfunc,
                                          group_by_cols=group_by_cols,
                                          group_by=group_by,
                                          chunksize=10000)
            print(f'[+] "pivot_table", output: {output}')

        if job.get("type") == "process_csv_remove_parentheses":
            print_job_start(job_index, total_jobs, job_name, job)
            job_matched = True

            input_csv = job.get("input_csv")
            index_cols = job.get("columns")
            edit_in_place = job.get("edit_in_place", False)
            output = process_csv_remove_parentheses(input_csv=input_csv,
                                                    columns=index_cols,
                                                    edit_in_place=edit_in_place,
                                                    chunksize=10000)
            print(f'[+] "process_csv_remove_parentheses", output: {output}')

        # display progress
        if not job_matched:
            print(
                f"{Fore.YELLOW}[-] Warning: The job type '{job.get('type')}' is not recognized. Skipping this job."
                f"{Style.RESET_ALL}")

        print(f"Job {job_index + 1}/{total_jobs} -[{job_name}] completed.  Type ::{job.get('type')}::\n")


if __name__ == "__main__":
    main()
