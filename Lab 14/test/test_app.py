
from sympy import exp
from app import *
import pytest

def test_hello():
    got = hello("Aleksandra")
    want = "Hello Aleksandra"

    assert got == want

testdata1 = ["I think today will be a great day"]

@pytest.mark.parametrize('sample', testdata1)
def test_extract_sentiment(sample):

    sentiment = extract_sentiment(sample)

    assert sentiment > 0

testdata2 = [
    ('There is a duck in this text', 'duck', True),
    ('There is nothing here', 'duck', False)
    ]

@pytest.mark.parametrize('sample, word, expected_output', testdata2)
def test_text_contain_word(sample, word, expected_output):

    assert text_contain_word(word, sample) == expected_output


test_data = [
    ([5, 6, 2, 1, 4, 3], [1, 2, 3, 4, 5, 6], True),
    ([-12, -3, 2, 0, 20, 10], [-12, -3, 0, 2, 10, 20], True),
    ([-100, 25, -10, 5, 0, 3, 2, 1], [-100, -10, 0, 1, 2, 3, 5, 25], True)
]

@pytest.mark.parametrize("sample, expected_output, correctness", test_data)
def test_bubblesort(sample, expected_output, correctness):
    got = bubblesort(sample)
    want = expected_output, correctness

    assert got == want
