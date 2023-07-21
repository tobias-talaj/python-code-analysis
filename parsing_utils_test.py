import ast
import json
import pytest

from parsing_utils import remove_merge_conflicts, parse_notebook, is_ipynb, extract_code, cleanup_extracted_code, parse_to_ast


@pytest.fixture
def example_code():
    return 'def example():\n    print("Hello, world!")'


@pytest.fixture
def example_notebook(example_code):
    return json.dumps({
        "cells": [{
            "cell_type": "code",
            "source": [example_code]
        }]
    })


@pytest.mark.parametrize("input_code, expected_output", [
    ("""<<<<<<< HEAD
example_code
||||||| merged common ancestors
example_code
=======
example_code
>>>>>>>""", "example_code"),
    ("example_code", "example_code")
])
def test_remove_merge_conflicts(input_code, expected_output, example_code):
    assert remove_merge_conflicts(input_code.replace("example_code", example_code)) == expected_output.replace("example_code", example_code)


def test_parse_notebook(example_notebook, example_code):
    assert parse_notebook(example_notebook) == example_code + '\n'


@pytest.mark.parametrize("input_code, expected_output", [
    ("example_notebook", True),
    ("example_code", False)
])
def test_is_ipynb(input_code, expected_output, example_code, example_notebook):
    assert is_ipynb(locals()[input_code]) == expected_output


def test_extract_code():
    test_chunk = 'aaaa1111bbbb2222cc33d4e5f6g7h8i9j0k1l2m3,1234,"def example():\n    print("Hello, world!")'
    assert extract_code(test_chunk) == ('def example():\n    print("Hello, world!")', 'aaaa1111bbbb2222cc33d4e5f6g7h8i9j0k1l2m3,1234,"')


@pytest.mark.parametrize("chunk_id, expected_output", [
    ("d85626964c4991f63f841afe6a28564559f8c4e5", ""),
    ("non_omitted_id", "example_code")
])
def test_cleanup_extracted_code(chunk_id, expected_output, example_code):
    assert cleanup_extracted_code(f"{example_code}", chunk_id) == expected_output.replace("example_code", example_code)


@pytest.mark.parametrize("input_code, expected_output", [
    ("example_code", ast.AST),
    ("""non-valid-code""", ast.AST)
])
def test_parse_to_ast(input_code, expected_output):
    input_code = locals().get(input_code, input_code)
    assert isinstance(parse_to_ast(input_code), expected_output)
