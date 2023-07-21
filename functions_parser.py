"""
A script to count all accourences of Python builtin functions in given files. 
"""


import os
import re
import ast
import time
import string
import logging
import collections
import multiprocessing
from pathlib import Path

from parsing_utils import cleanup_extracted_code, parse_to_ast, save_counts_to_parquet, concatenate_parquet_files,\
    configure_utils_logging, is_ipynb, extract_code, parse_notebook


def get_builtin_functions():
    return [i for i in dir(__builtins__) if any(i.startswith(j) for j in string.ascii_lowercase)]


def configure_logging(filename, level):
    logging.basicConfig(filename=filename, level=level)
    configure_utils_logging(filename=filename, level=level)


def find_functions(node, counts, builtin_functions):
    """
    Recursively traverses the abstract syntax tree starting at the given node and counts the number of times
    built-in functions are used, regardless of context.

    Args:
        node (ast.AST): The root node of the abstract syntax tree to traverse.
        counts (dict): A dictionary to store the counts of built-in function usage.
        builtin_functions (list): A list of built-in function names to search for in the abstract syntax tree.

    Side Effects:
        Updates the counts dictionary with the number of times each built-in function is used in the abstract syntax
        tree starting at the given node.

    Returns:
        None
    """
    if isinstance(node, (ast.Attribute, ast.Name)):
        if isinstance(node, ast.Attribute):
            func_name = node.attr
        elif isinstance(node, ast.Name):
            func_name = node.id
        if func_name in builtin_functions:
            counts[func_name] += 1
        for _, child_node in ast.iter_fields(node):
            if isinstance(child_node, ast.AST):
                find_functions(child_node, counts, builtin_functions)
            elif isinstance(child_node, list):
                for child in child_node:
                    if isinstance(child, ast.AST):
                        find_functions(child, counts, builtin_functions)
    elif isinstance(node, ast.Call):
        find_functions(node.func, counts, builtin_functions)
    else:
        for _, child_node in ast.iter_fields(node):
            if isinstance(child_node, ast.AST):
                find_functions(child_node, counts, builtin_functions)
            elif isinstance(child_node, list):
                for child in child_node:
                    if isinstance(child, ast.AST):
                        find_functions(child, counts, builtin_functions)


def process_chunk(code_chunk):
    """
    Processes the given code chunk to find the built-in function counts.

    Args:
        code_chunk (str): The code chunk to process.

    Returns:
        dict: A dictionary containing the built-in function counts for the code chunk.
    """
    extracted_code, id_and_size = extract_code(code_chunk)
    if not extracted_code:
        return {}
    chunk_id = id_and_size.split(',')[0]
    cleaned_code = cleanup_extracted_code(extracted_code, chunk_id)
    is_jupyter = is_ipynb(cleaned_code)
    
    code_tree = parse_to_ast(parse_notebook(cleaned_code)) if is_jupyter else parse_to_ast(cleaned_code)
    
    builtin_counts = collections.defaultdict(int)
    try:
        find_functions(code_tree, builtin_counts, get_builtin_functions())
    except RecursionError as e:
        logging.debug(f"{chunk_id}: {e}")
    except Exception as e:
        logging.debug(f"{chunk_id}: {e}")

    return dict(chunk_id=chunk_id, **builtin_counts)


def process_file(text_file, threadcount):
    """
    Processes the given text file to find the built-in function counts.
    The text file is split into chunks and processed in parallel.

    Args:
        text_file (Path): The path of the text file to process.
        threadcount (int): The number of threads to use for parallel processing.

    Returns:
        list: A list of dictionaries, each containing the built-in function counts for a code chunk.
    """
    function_counts = []

    start_time = time.time()
    logging.debug(f"Processing text file {text_file}")

    with open(text_file, 'r', encoding='utf8') as f:
        content = f.read().replace('\x00', '')
        code_chunks = re.split(r'",false,\d+', content[30:])

    with multiprocessing.Pool(threadcount) as pool:
        results = [pool.apply_async(process_chunk, args=(chunk,)) for chunk in code_chunks]
        function_counts = [result.get() for result in results]

    end_time = time.time()
    logging.debug(f"Finished processing text file {text_file} in {end_time - start_time:.2f} seconds")

    return function_counts


def main():
    configure_logging(filename='functions_parser_debug.log', level=logging.INFO)
    text_input_directory = "/workspaces/repos/github_dump"
    parquet_output_directory = "/workspaces/repos/parquet_builtin_counts"
    final_output_directory = "/workspaces/repos/randomstats/github"
    final_filename = "functions_counts.parquet"
    threadcount = os.cpu_count()

    for text_file in Path(text_input_directory).glob('*.txt'):
        function_counts = process_file(text_file, threadcount)
        file_suffix = str(text_file)[-7:-4]
        output_filename = f"builtin_counts_{file_suffix}.parquet"
        save_counts_to_parquet(function_counts, parquet_output_directory, output_filename)

    concatenate_parquet_files(parquet_output_directory, final_output_directory, final_filename)


if __name__ == "__main__":
    main()
