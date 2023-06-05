import ijson
import json
from typing import Any, Dict, List, Union, Optional
from utils import count_items, get_datetime, find_root_key, CustomJSONEncoder
from tqdm import tqdm


# function to combine the current json object with new key:value pairs or any new nested key:value pairs
def combine_json_objects(obj1: Dict[str, Any], obj2: Dict[str, Any],
                         ignore_new_array_indices: bool = True) -> Dict[str, Any]:
    for key, value in obj2.items():
        if key in obj1:
            if isinstance(value, dict):
                obj1[key] = combine_json_objects(obj1[key], value, ignore_new_array_indices)
            elif isinstance(value, list) and isinstance(obj1[key], list):
                if not ignore_new_array_indices:
                    obj1[key].extend(value)
        else:
            obj1[key] = value
    return obj1


def build_example_json(input_json: Union[str, Dict], root_key: Optional[str] = None,
                       ignore_new_array_indices: bool = True):
    # Get the total number objects in the input json file (no matter how large)
    print(f'[+] Parsing -> {input_json}')
    if find_root_key(input_json):
        total_items = count_items(input_json, root_key)
    else:
        print(f'[X] Root Key NOT Found for {input_json}')

    example_json = {}

    with open(input_json, 'r', encoding='utf-8') as f:
        parser = ijson.items(f, f"{root_key}.item" if root_key else 'item')
        datetime = str(get_datetime())
        json_output_filename = 'build_example_json__' + root_key + '_' + datetime + ".json"

        for obj in tqdm(parser, total=total_items, desc='Processing objects', unit=' objects', ncols=100):
            example_json = combine_json_objects(example_json, obj, ignore_new_array_indices)

    with open(json_output_filename, 'w+', newline='', encoding='utf-8') as json_output:
        json.dump(example_json, json_output, ensure_ascii=False, indent=2, cls=CustomJSONEncoder)

    return json_output_filename