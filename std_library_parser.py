"""
This file contains functions that extract and count Python library components in a collection of code files, and write the output to a Parquet file.
The program reads in a dictionary of standard library components from a pickle file, and can be run as a script or imported as a module.
"""


import re
import os
import sys
import ast
import time
import pickle
import logging
import multiprocessing
from pathlib import Path
from collections import Counter
from collections import defaultdict

import pandas as pd


def extract_imports(contents):
    """Extracts import statements from Python code.

    Args:
        contents (str): The contents of a Python code file.

    Returns:
        dict: A dictionary of imports, where the keys are the module names and the values
            are tuples containing the alias name (if any) and a list of direct imports (if any).
    """
    try:
        code_tree = ast.parse(contents)
    except SyntaxError as e:
        code_tree = None
        logging.debug(f"{e} (most probably Python 2 code)")
        return {}
    except Exception as e:
        code_tree = None
        logging.debug(f"{e}")
        return {}

    imports = {}

    for node in code_tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                alias_name = alias.asname if alias.asname else alias.name
                imports[alias.name] = (alias_name, [])
        elif isinstance(node, ast.ImportFrom):
            if node.module not in imports:
                imports[node.module] = (node.module, [])
            for alias in node.names:
                alias_name = alias.asname if alias.asname else alias.name
                imports[node.module][1].append(alias_name)

    return imports


def get_components_to_search(code_chunk_imports, standard_library_dict):
    """Creates a collection of components to search for in a code chunk by including only the components of imported libraries.

    Args:
        code_chunk_imports (dict): A dictionary of imports extracted from a code chunk.
        standard_library_dict (dict): A dictionary of standard library components to search for.

    Returns:
        dict: A dictionary of standard library components to search for in the code chunk,
            filtered by the imports in the code chunk.
    """
    filtered = {}
    for std_library_name, std_lib_components in standard_library_dict.items():
        if std_library_name in code_chunk_imports:
            std_library_alias_name = code_chunk_imports[std_library_name][0]
            if code_chunk_imports[std_library_name][1]:
                filtered[(std_library_name, std_library_alias_name)] = defaultdict(list)
                if '*' in code_chunk_imports[std_library_name][1]:
                    filtered[(std_library_name, std_library_alias_name)] = {**filtered[(std_library_name, std_library_alias_name)], **{f'from_import_{key}': value for key, value in std_lib_components.copy().items()}}
                else:
                    for direct_import in code_chunk_imports[std_library_name][1]:
                        for component in std_lib_components:
                            if direct_import in std_lib_components[component]:
                                filtered[(std_library_name, std_library_alias_name)][f'from_import_{component}'].append(direct_import)
            else:
                filtered[(std_library_name, std_library_alias_name)] = std_lib_components.copy()
    return filtered


def count_library_components(code_chunk, library_dict):
    """Counts the occurrences of standard library components in a code chunk.

    Args:
        code_chunk (str): A code chunk.
        library_dict (dict): A dictionary of standard library components to search for.

    Returns:
        dict: A dictionary of component counts, where the keys are the IDs of the code chunks
            and the values are dictionaries of standard library component counts, where the keys
            are the library names and the values are dictionaries of component type counts, where
            the keys are the component types and the values are Counter objects containing the
            counts of component names.
    """
    search_result = re.search(r'[a-z0-9]{40},\d+,"', code_chunk)
    if search_result is None:
        return {}
    id_and_size = search_result.group()
    chunk_id, _ = id_and_size.split(',')[:2]

    code_chunk = code_chunk.replace(id_and_size, '').replace('""', '"')

    import_statements = extract_imports(code_chunk)
    components_to_search = get_components_to_search(import_statements, library_dict)

    library_component_counts = defaultdict(dict)
    for library_name, components in components_to_search.items():
        library_name, alias_name = library_name
        for component_type in components:
            if components[component_type]:
                components_list = components[component_type]
            else:
                continue

            matches = []
            if component_type in ('function', 'exception'):
                pattern = r'{}\.({})'.format(alias_name, '|'.join(components_list))
                matches = re.findall(pattern, code_chunk)
            elif component_type in ('method', 'class', 'attribute'):
                pattern = r'\.(?:{})'.format('|'.join(components_list))
                matches = [f[1:] for f in re.findall(pattern, code_chunk)] if re.findall(pattern, code_chunk) else None
            else:
                pattern = r'(?<!\.)\b(?:{})\b'.format('|'.join(components_list))
                matches = re.findall(pattern, code_chunk)
            if matches:
                library_component_counts[library_name][component_type] = Counter(matches)
    return {chunk_id: library_component_counts}


def process_file(text_file, threadcount, library_dict):
    """Processes a text file containing code chunks.

    Args:
        text_file (str): The path to the text file.
        threadcount (int): The number of threads to use for processing.
        library_dict (dict): A dictionary of standard library components to search for.

    Returns:
        list: A list of dictionaries of component counts, where the keys are the IDs of the code chunks
            and the values are dictionaries of standard library component counts, where the keys
            are the library names and the values are dictionaries of component type counts, where
            the keys are the component types and the values are Counter objects containing the
            counts of component names.
    """
    start_time = time.time()
    logging.debug(f"Processing text file {text_file}")

    with open(text_file, 'r', encoding='utf8') as f:
        content = f.read().replace('\x00', '')
        code_chunks = re.split(r'",false,\d+', content[30:])

    with multiprocessing.Pool(threadcount) as pool:
        results = [pool.apply_async(count_library_components, args=(chunk, library_dict)) for chunk in code_chunks]
        function_counts = [result.get() for result in results]
        function_counts_dict = {}
        for d in function_counts:
            function_counts_dict.update(d)

    end_time = time.time()
    logging.info(f"Finished processing text file {text_file} in {end_time - start_time:.2f} seconds")

    return function_counts


def save_function_counts_to_parquet(function_counts, output_filename, output_directory):
    """Saves component counts to a Parquet file.

    Args:
        function_counts (list): A list of dictionaries of component counts, where the keys are the IDs
            of the code chunks and the values are dictionaries of standard library component counts,
            where the keys are the library names and the values are dictionaries of component type counts,
            where the keys are the component types and the values are Counter objects containing the counts
            of component names.
        output_filename (str): The name of the output file.
        output_directory (str): The path to the output directory.
    """
    start_time = time.time()

    data = [
        {
            'chunk_id': chunk_id,
            'library_name': library_name,
            'component_type': component_type,
            'component': str(component),
            'count': count
        }
        for function_count in function_counts
        for chunk_id, libraries in function_count.items()
        for library_name, components in libraries.items()
        for component_type, component_counts in components.items()
        for component, count in component_counts.items()
    ]
    df = pd.DataFrame(data)

    os.makedirs(output_directory, exist_ok=True)
    file_path = os.path.join(output_directory, output_filename)
    df.to_parquet(file_path, compression='snappy')
    end_time = time.time()
    logging.info(f"Saved Parquet file {output_filename} in {end_time - start_time:.2f} seconds")


def concatenate_parquet_files(input_directory, output_directory, output_filename):
    """Concatenates multiple Parquet files into a single Parquet file.

    Args:
        input_directory (str): The path to the input directory.
        output_directory (str): The path to the output directory.
        output_filename (str): The name of the output file.
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    parquet_files = [file for file in os.listdir(input_directory) if file.endswith('.parquet')]
    combined_df = pd.concat([pd.read_parquet(os.path.join(input_directory, file)) for file in parquet_files], axis=0)
    combined_df.to_parquet(os.path.join(output_directory, output_filename), compression='snappy')


def main():
    logging.basicConfig(filename='debug.log', level=logging.INFO)
    sys.setrecursionlimit(3000)
    parquet_output_directory = "/workspaces/repos/parquet_library_counts"
    final_output_directory = "/workspaces/repos/randomstats/github"
    output_filename  = "library_counts.parquet"
    text_input_directory = "/workspaces/repos/github_dump"
    library_pickle_path = "/workspaces/repos/randomstats/github/standard_library_api_dict.pickle"

    with open(library_pickle_path, 'rb') as f:
        library_dict = pickle.load(f)

    threadcount = os.cpu_count()

    for text_file in Path(text_input_directory).glob('*.txt'):
        function_counts = process_file(text_file, threadcount, library_dict)
        file_suffix = str(text_file)[-7:-4]
        output_filename_with_suffix = f"library_counts_{file_suffix}.parquet"
        save_function_counts_to_parquet(function_counts, output_filename_with_suffix, parquet_output_directory)

    concatenate_parquet_files(parquet_output_directory, final_output_directory, output_filename)


if __name__ == "__main__":
    main()
