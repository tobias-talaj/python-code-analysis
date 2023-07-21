import pickle
from collections import Counter

from std_library_parser import extract_imports, get_components_to_search, count_library_components


code_example = """
import math
import random
import json
import re
from collections import namedtuple, defaultdict
from functools import reduce
import sys as system
import os
from os import *

sqrt = math.sqrt(16)
rnd = random.random()
random.seed(42)
data = '{"name": "John", "age": 30, "city": "New York"}'
try:
    parsed_data = json.loads(data)
except json.JSONDecodeError as e:
    print(f"Error while decoding JSON: {e}")

pattern = re.compile(r"\\d+")
matched_digits = pattern.findall("Hello 12345 World 67890")

Person = namedtuple("Person", "name age")
person = Person("Alice", 30)
name, age = person

word_count = defaultdict(int)
text = "apple banana apple orange banana apple"
for word in text.split():
    word_count[word] += 1

product = reduce(lambda x, y: x * y, [1, 2, 3, 4])

platform = system.platform
system.exit(0)

current_directory = getcwd()
file_list = os.listdir()
"""


with open('/workspaces/repos/randomstats/github/standard_library_api_dict.pickle', 'rb') as f:
    standard_library_dict = pickle.load(f)


def test_extract_imports():
    imports = extract_imports(code_example)
    expected_imports = {
        "math": ("math", []),
        "random": ("random", []),
        "json": ("json", []),
        "re": ("re", []),
        "collections": ("collections", ["namedtuple", "defaultdict"]),
        "functools": ("functools", ["reduce"]),
        "sys": ("system", []),
        "os": ("os", ["*"]),
    }
    assert imports == expected_imports


def test_get_components_to_search():
    code_imports = extract_imports(code_example)
    components_to_search = get_components_to_search(code_imports, standard_library_dict)

    expected_components_to_search = {
        ("math", "math"): standard_library_dict["math"],
        ("random", "random"): standard_library_dict["random"],
        ("json", "json"): standard_library_dict["json"],
        ("re", "re"): standard_library_dict["re"],
        ("collections", "collections"): {
            "from_import_function": ['namedtuple'],
            "from_import_class": ['defaultdict'],
        },
        ("functools", "functools"): {
            "from_import_function": ['reduce'],
        },
        ("sys", "system"): standard_library_dict["sys"],
        ("os", "os"): {
            **standard_library_dict["os"],
            "from_import_function": standard_library_dict["os"]['function'],
            "from_import_method": standard_library_dict["os"]['method'],
            "from_import_class": standard_library_dict["os"]['class'],
            "from_import_exception": standard_library_dict["os"]['exception'],
            "from_import_attribute": standard_library_dict["os"]['attribute'],
        },
    }

    # Plain imports
    for module in [("math", "math"), ("random", "random"), ("json", "json"), ("re", "re")]:
        assert components_to_search[module] == expected_components_to_search[module]

    # From imports
    for module in [("collections", "collections"), ("functools", "functools")]:
        for key, value in expected_components_to_search[module].items():
            if key.startswith("from_import_"):
                assert components_to_search[module][key] == value

    # Alias imports
    assert components_to_search[("sys", "system")] == expected_components_to_search[("sys", "system")]

    # Asterisk imports
    for key, value in expected_components_to_search[("os", "os")].items():
        if key.startswith("from_import_"):
            assert components_to_search[("os", "os")][key] == value


def test_count_library_components():
    chunk_id = "0000000000000000000000000000000000000000"
    code_chunk = f'{chunk_id},0,"{code_example}'
    library_component_counts = count_library_components(code_chunk, standard_library_dict)
    expected_result = {
        "math": {"function": Counter({"sqrt": 1})},
        "random": {"function": Counter({"random": 1, "seed": 1})},
        "json": {"function": Counter({"loads": 1}), "exception": Counter({"JSONDecodeError": 1})},
        "re": {"function": Counter({"compile": 1}), "method": Counter({"findall": 1, "split": 1})},
        "collections": {"from_import_function": Counter({"namedtuple": 2}), "from_import_class": Counter({"defaultdict": 2})},
        "functools": {"from_import_function": Counter({"reduce": 2})},
        "sys": {"function": Counter({"exit": 1})},
        "os": {"from_import_function": Counter({"system": 3, "getcwd": 1}), "from_import_attribute": Counter({"name": 3})},
    }
    assert library_component_counts[chunk_id] == expected_result
