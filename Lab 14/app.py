from textblob import TextBlob
import numpy as np


def hello(name):
    output = f'Hello {name}'
    return output


def extract_sentiment(text):
    text = TextBlob(text)

    return text.sentiment.polarity

def text_contain_word(word: str, text: str):
    return word in text


def bubblesort(elements):
    not_sorted = elements
    for n in range(len(elements)-1, 0, -1):
        for i in range(n):
            if elements[i] > elements[i + 1]:
                elements[i], elements[i + 1] = elements[i + 1], elements[i]
    if elements == sorted(not_sorted):
        correct = True
    else:
        correct = False
    return elements, correct
