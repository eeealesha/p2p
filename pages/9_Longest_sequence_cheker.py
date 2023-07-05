import streamlit as st

def longest_sequence(word):
    max_length = 0
    current_length = 1
    longest_symbol = ""
    current_symbol = ""

    for i in range(1, len(word)):
        if word[i] == word[i - 1]:
            current_length += 1
            current_symbol = word[i]
        else:
            if current_length > max_length:
                max_length = current_length
                longest_symbol = current_symbol
            current_length = 1
            current_symbol = ""

    # Check if the last sequence is the longest
    if current_length > max_length:
        max_length = current_length
        longest_symbol = current_symbol

    return longest_symbol, max_length

st.title("Longest Sequence of Equal Symbols")

# User input for the word
word = st.text_input("Enter a word")

# Calculate the longest sequence of equal symbols
if st.button("Calculate"):
    symbol, length = longest_sequence(word)
    st.write(f"The longest sequence of equal symbols in {word} is '{symbol}' with a length of {length}.")
