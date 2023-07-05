import streamlit as st

def is_palindrome(word):
    # Remove spaces and convert to lowercase
    word = word.replace(" ", "").lower()
    # Check if the word is equal to its reverse
    return word == word[::-1]

st.title("Palindrome Checker")

    # User input for the word
word = st.text_input("Enter a word")

# Check if the word is a palindrome
if st.button("Check Palindrome"):
    if is_palindrome(word):
        st.write(f"{word} is a palindrome!")
    else:
        st.write(f"{word} is not a palindrome.")