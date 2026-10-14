"""extract_json finds the answer object when the model writes more than it.

An agent run often shows code in a fence before it gives its answer in a
second fence. Only the first fence was read, and the fallback scan failed on
the closing fence marker after the answer, so the answer was lost.
"""
from core.llm import extract_json


def test_the_answer_fence_after_a_code_fence_is_read():
    text = (
        "The selector is:\n```\nCallExpression > Arg:first-child\n```\n\n"
        "Here is the answer.\n```json\n{\"valid\": \"partly\", \"explanation\": \"two gaps\"}\n```\n"
    )
    assert extract_json(text) == {"valid": "partly", "explanation": "two gaps"}


def test_an_answer_followed_by_prose_is_read_whole():
    text = 'Done.\n{"valid": "yes", "detail": {"line": 3}}\nThat is my answer.'
    assert extract_json(text) == {"valid": "yes", "detail": {"line": 3}}


def test_a_single_fenced_answer_still_wins():
    assert extract_json('```json\n{"a": 1}\n```') == {"a": 1}


def test_text_with_no_object_is_none():
    assert extract_json("no json here {not json") is None
