import ast
import pytest
from metadata_parser import process_chunk, find_occurences


def test_find_occurences():
    source_code = "def test_func(): a = 1 + 2; b = a * 3; c = b.attr; d = e.func()"
    node = ast.parse(source_code)
    counts = {'calls': 0, 'assignments': 0, 'attributes': 0}
    find_occurences(node, counts)
    assert counts == {'calls': 1, 'assignments': 4, 'attributes': 2}


@pytest.mark.parametrize(
    "code_chunk,expected",
    [
        (
            'ktu8qfsr2gv4myex6canjhblpz3dwoi75esatarm,38,"a = 1 + 2; b = a * 3; c = b.attr; d = e.func()',
            {
                'chunk_id': 'ktu8qfsr2gv4myex6canjhblpz3dwoi75esatarm',
                'calls': 1,
                'assignments': 4,
                'attributes': 2,
                'size': 46,
                'is_ipynb': False
            }
        ),
    ]
)
def test_process_chunk(code_chunk, expected):
    result = process_chunk(code_chunk)
    assert result == expected
