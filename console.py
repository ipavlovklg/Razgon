"""
Утилиты консоли.
"""

import os


is_new_line = True

def write_line(message: str):
    """
    Writes a message in the console and starts a new line.
    If no new line before the message, it will be added.
    """
    global is_new_line
    if not is_new_line:
        print()
    print(message)
    is_new_line = True

def write(message):
    """Writes a message in the console without a new line before and after."""
    global is_new_line
    print(message, end="", flush=True)
    is_new_line = False

def clear():
    """Clears the console."""
    os.system('cls' if os.name == 'nt' else 'clear')