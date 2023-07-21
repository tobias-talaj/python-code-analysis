"""This script analyzes a dataset of Python code files and Jupyter notebooks, stored as text files, to generate
metadata about each file. It counts the number of function calls, assignments, and attributes in each file, as well as
determining the file size and whether it's a Jupyter notebook. The metadata is saved in Parquet format for further analysis.
"""


import os
import re
import sys
import ast
import time
import logging
import multiprocessing
from pathlib import Path

from parsing_utils import cleanup_extracted_code, parse_to_ast, save_counts_to_parquet, concatenate_parquet_files,\
    configure_utils_logging, is_ipynb, extract_code, parse_notebook


def configure_logging(filename, level):
    logging.basicConfig(filename=filename, level=level)
    configure_utils_logging(filename=filename, level=level)


def find_occurences(node, counts):
    """
    Recursively searches the AST for function calls, assignments, and attribute accesses, updating the counts
    dictionary with the number of occurrences for each.

    Args:
        node (ast.AST): The current AST node to search.
        counts (dict): A dictionary containing the counts of calls, assignments, and attributes.
    """
    try:
        if isinstance(node, ast.Call):
            counts['calls'] += 1
        elif isinstance(node, ast.Assign):
            counts['assignments'] += 1
        elif isinstance(node, ast.Attribute):
            counts['attributes'] += 1

        for field in getattr(node, '_fields', []):
            value = getattr(node, field)
            if isinstance(value, list):
                for item in value:
                    find_occurences(item, counts)
            elif value is not None:
                find_occurences(value, counts)
    except RecursionError as e:
        logging.debug(f"{e} (Recursion error)")


def process_chunk(code_chunk):
    """
    Processes a code chunk, extracting metadata and returning it as a dictionary.

    Args:
        code_chunk (str): A string containing a code chunk from a text file.

    Returns:
        dict: A dictionary containing the metadata of the code chunk.
    """
    extracted_code, id_and_size = extract_code(code_chunk)
    if not extracted_code:
        return {}
    chunk_id = id_and_size.split(',')[0]
    # chunk_size = id_and_size.split(',')[1]  # it includes non-code parts of Jupyter notebooks
    cleaned_code = cleanup_extracted_code(extracted_code, chunk_id)

    if is_jupyter := is_ipynb(cleaned_code):
        cleaned_code = parse_notebook(cleaned_code)
    
    code_tree = parse_to_ast(cleaned_code)

    counts = {'calls': 0, 'assignments': 0, 'attributes': 0}
    find_occurences(code_tree, counts)
    counts['size'] = len(cleaned_code)
    counts['is_ipynb'] = is_jupyter

    return dict(chunk_id=chunk_id, **counts)


def process_file(text_file, threadcount):
    """
    Processes a text file containing code chunks, extracting metadata for each chunk using multiple threads.

    Args:
        text_file (str): The path to the text file containing the code chunks.
        threadcount (int): The number of threads to use for processing the code chunks.

    Returns:
        list: A list of dictionaries containing the metadata for each code chunk.
    """
    start_time = time.time()
    logging.debug(f"Processing text file {text_file}")

    with open(text_file, 'r', encoding='utf8') as f:
        content = f.read().replace('\x00', '')
        code_chunks = re.split(r'",false,\d+', content[30:])

    with multiprocessing.Pool(threadcount) as pool:
        results = [pool.apply_async(process_chunk, args=(chunk,)) for chunk in code_chunks]
        metadata = [result.get() for result in results]

    end_time = time.time()
    logging.debug(f"Finished processing text file {text_file} in {end_time - start_time:.2f} seconds")

    return metadata


def main():
    configure_logging(filename='metadata_parser_debug.log', level=logging.DEBUG)
    sys.setrecursionlimit(3000)
    text_input_directory = "/workspaces/repos/github_dump"
    parquet_output_directory = "/workspaces/repos/metadata"
    final_output_directory = "/workspaces/repos/randomstats/github"
    final_filename = "metadata.parquet"
    threadcount = os.cpu_count()

    for text_file in Path(text_input_directory).glob('*.txt'):
        counts = process_file(text_file, threadcount)
        file_suffix = str(text_file)[-7:-4]
        output_filename = f"metadata_{file_suffix}.parquet"
        save_counts_to_parquet(counts, parquet_output_directory, output_filename)

    concatenate_parquet_files(parquet_output_directory, final_output_directory, final_filename)

if __name__ == '__main__':
    main()