import re

from numpy import median
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from tests import balance_result
from tests.label import jaccard_similarity

__author__ = 'alse'


def word2vec_cosine_test(train_vec, test_vec):
    if len(train_vec) == 0 or len(test_vec) == 0:
        return 0
    return cosine_similarity(train_vec, test_vec)


def jaccard_test(train_examples, test_examples, num1, num2):
    result = jaccard_similarity(train_examples, test_examples)
    return balance_result(num1, num2, False, result)


def cosine_test(train_text, test_text, num1, num2):
    if not train_text or not test_text:
        return 0.0
    tfidf_vectorizer = TfidfVectorizer()
    tfidf_matrix = tfidf_vectorizer.fit_transform([train_text.lower(), test_text.lower()])
    result = (tfidf_matrix * tfidf_matrix.T).A[0, 1]
    return balance_result(num1, num2, False, result)


def char_len_test(train_lengths, test_lengths, num1, num2):
    avg_train_length = median(train_lengths)
    avg_test_length = median(test_lengths)
    if avg_test_length == 0 or avg_train_length == 0:
        return False
    else:
        return avg_test_length == avg_train_length


def len_test(train_lengths, test_lengths, num1, num2):
    avg_train_length = median(train_lengths)
    avg_test_length = median(test_lengths)
    if avg_test_length == 0 or avg_train_length == 0:
        return False
    else:
        return avg_test_length == avg_train_length


def get_abbr_patterns(abbr):
    if not abbr or len(abbr) > 5:
        return []

    abbr = re.sub(r'\p{P}', "", abbr)
    patterns = [r"\b"] * 4
    for char in abbr[:-1]:
        patterns[0] += (char + r"[a-z]+\p{Z}+")
        patterns[1] += (char + r"[a-z]*")
        patterns[2] += (char + r"[a-z]*?\p{Z}")
    patterns[0] += (abbr[-1] + r"[a-z]+\b")
    patterns[1] += (abbr[-1] + r"[a-z]*\b")
    patterns[2] += (abbr[-1] + r"[a-z]*\b")
    patterns[3] += (abbr + r"[a-z]+\b")

    for idx, pattern in enumerate(patterns):
        try:
            patterns[idx] = re.compile(pattern, re.IGNORECASE)
        except Exception:
            patterns[idx] = re.compile("", re.IGNORECASE)

    return patterns


def abbr_test(train_examples, test_examples, num1, num2):
    # if testExamples is a string, perform metadata abbr test (label thing). Else do the normal one
    train_example_set = set(train_examples)

    count_matches = 0

    if isinstance(test_examples, str):
        patterns = get_abbr_patterns(test_examples)
        for pattern in patterns:
            if pattern.match(train_examples):
                count_matches += 1
                break
        return count_matches
    else:
        test_example_set = set(test_examples)

        if len(test_example_set) > 50 or len(train_example_set) > 50:
            return 0.0

        for test_example in test_example_set:
            if not test_example.isupper():
                continue
            patterns = get_abbr_patterns(test_example)
            found = False
            for pattern in patterns:
                for train_example in train_example_set:
                    if pattern.match(train_example):
                        count_matches += 1
                        found = True
                        break
                    if found:
                        break
        result = count_matches * 2.0 / (len(train_example_set) + len(test_example_set))
        return balance_result(num1, num2, False, result)
