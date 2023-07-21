import re
import os
import ast
import json
import logging

import pandas as pd


def configure_utils_logging(filename, level):
    logging.basicConfig(filename=filename, level=level)


def remove_merge_conflicts(code):
    """
    Remove merge conflicts from code.

    Args:
        code (str): A string containing code.

    Returns:
        str: The cleaned up code without merge conflicts.
    """
    code_lines = code.splitlines()
    cleaned_code = []
    in_conflict = False

    for line in code_lines:
        if re.match(r'^<<<<<<<', line):
            in_conflict = True
            continue
        elif re.match(r'^=======', line):
            in_conflict = False
            continue
        elif re.match(r'^>>>>>>>', line):
            continue
        if not in_conflict:
            cleaned_code.append(line)

    return '\n'.join(cleaned_code)


def parse_notebook(code):
    """
    Extracts code from a Jupyter notebook.

    Args:
        code (str): A string containing a Jupyter notebook.

    Returns:
        str: The extracted code.
    """
    try:
        extracted = ''
        for cell in json.loads(code)['cells']:
            if cell['cell_type'] == 'code' and cell['source'] != ['']:
                for line in cell['source']:
                    if '%' in line[:1]:
                        continue
                    extracted += line
                extracted += '\n'
        return extracted
    except Exception as e:
        logging.debug(f"{e} (Notebook parsing error)")
        return ''


def is_ipynb(code):
    """
    Determines if a string contains a Jupyter notebook.

    Args:
        code (str): A string.

    Returns:
        bool: True if the string contains a Jupyter notebook, False otherwise.
    """
    if ('"cell_type": "code"' in code) or ('"cell_type": "markdown"' in code):
        return True
    return False


def extract_code(code_chunk):
    """
    Extracts code and ID/size from a code chunk.

    Args:
        code_chunk (str): A string containing a code chunk.

    Returns:
        tuple: A tuple containing the code and the ID/size.
    """
    id_size_search_result = re.search(r'[a-z0-9]{40},\d+,"', code_chunk)
    if id_size_search_result is None:
        return '', ''
    id_and_size = id_size_search_result.group()
    return code_chunk.replace(id_and_size, ''), id_and_size


def cleanup_extracted_code(extracted_code, chunk_id):
    """
    Cleans up extracted code by removing redundant quotes and removing merge conflicts.

    Args:
        extracted_code (str): A string containing the extracted code.
        chunk_id (str): The ID of the code chunk.

    Returns:
        str: The cleaned up code.
    """
    if chunk_id in ('d85626964c4991f63f841afe6a28564559f8c4e5', '18d8b80f6f1e1d497d2356f1018756a0f6888320',
                   '54e8c1952323686e1779c21fcbfd4c857add857e', '3bdb277771a4c7ed55385846bc133b0768a17bba',
                   '21b296cde51a9f8d6667f6fd5a81e9125a59316e', '639acd34b2ae7cf9dbf93cb6c9a22552d4202a37',
                   '051a03a391f81f5eb0fe3dfc1f7d39ca96a499f6', 'c9a5a8ef568b6c867f1e84d84fcc1a6463a77637'):
        logging.info(f"Omitted {chunk_id}")
        return ''  # Weird behavior TODO
    
    extracted_code = extracted_code.replace('""', '"')
    return remove_merge_conflicts(extracted_code)


def parse_to_ast(cleaned_code):
    """
    Parses cleaned code to an abstract syntax tree.

    Args:
        cleaned_code (str): A string containing cleaned up code.

    Returns:
        ast.AST: The abstract syntax tree for the cleaned code.
    """
    try:
        return ast.parse(cleaned_code)
    except SyntaxError as e:
        logging.debug(f"{e} (Most probably Python 2 code)")
        return ast.parse('')
    except MemoryError as e:
        logging.debug(f"{e}")  # TODO
        return ast.parse('')
    except Exception as e:
        logging.debug(f"{e}")
        return ast.parse('')
    

def save_counts_to_parquet(counts, parquet_output_directory, output_filename):
    """
    Saves counts to a Parquet file.

    Args:
        counts (dict): A dictionary containing counts.
        parquet_output_directory (str): The directory where the Parquet file should be saved.
        output_filename (str): The name of the Parquet file.

    Returns:
        None
    """
    df = pd.DataFrame(counts)
    full_path = os.path.join(parquet_output_directory, output_filename)
    df.to_parquet(full_path)
    logging.info(f"Intermediate parquet file saved to {output_filename} in {parquet_output_directory}")


def concatenate_parquet_files(parquet_input_directory, final_output_directory, final_filename):
    """
    Concatenates multiple Parquet files into a single Parquet file.

    Args:
        parquet_input_directory (str): The directory containing the input Parquet files.
        final_output_directory (str): The directory where the final Parquet file should be saved.
        final_filename (str): The name of the final Parquet file.

    Returns:
        None
    """
    parquet_files = [os.path.join(parquet_input_directory, f) for f in os.listdir(parquet_input_directory) if f.endswith('.parquet')]
    df_list = []
    for file in parquet_files:
        df_list.append(pd.read_parquet(file))
    df = pd.concat(df_list)
    df.set_index('chunk_id', inplace=True)
    save_counts_to_parquet(df, final_output_directory, final_filename)
    logging.info(f"All parquet files concatenated and saved to {final_filename}")
