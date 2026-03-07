# - [ ]  **Variables & Data Types**
#     - Understanding `int`, `float`, `str`, `bool`.
#     - [Real Python: Variables in Python](https://realpython.com/python-variables/)
# - [ ]  **Control Flow (If/Else)**
#     - Conditional logic and indentation.
#     - [W3Schools: Python Conditions](https://www.w3schools.com/python/python_conditions.asp)
# - [ ]  **Loops**
#     - `for` loops, `while` loops, and `break`/`continue` statements.
#     - [Programiz: Python Loops](https://www.programiz.com/python-programming/for-loop)
# - [ ]  **Data Structures**
#     - Lists, Tuples, Sets, and Dictionaries.
#     - [Python Docs: Data Structures](https://docs.python.org/3/tutorial/datastructures.html)


# Variables & Data Types


# ============LIST==============
# List - mutuable

l = [1, 3.5, "Hello", True]

l.append(10)

# print(l)

l.remove(3.5)
# print(l)

l[0] = 100

# print(l)

# ==========================

# ============TUPLE==============

# Tuple - immutable

t = (1, 3.5, "Hello", True)

# t[0] = 100  # This will raise an error because tuples are immutable

# print(t)

# ==========================

# ============SET==============

# Set - mutable, unordered, no duplicates

s = {1, 3.5, "Hello", True}

s.add(10)

# print(s)

s.remove(3.5)
# print(s)

s.add(10)

# print(s)

# ==========================

# ============DICTIONARY==============

# Dictionary - mutable, key-value pairs

d = {
    "name": "Alice",
    "age": 30,
    "is_student": False,
    (1, 2): "This is a tuple key"
}

d["name"] = "Bob"  # Update value for existing key

# print(d)

# ==========================


# for item in l:
#     print(item)

# print(item)

# for key, value in d.items():
#     print(f"Key: {key}, Value: {value}")

l = [1, 3.5, "Hello", True]
index = 0

while True:

    if index >= len(l):
        print("End of list reached.")
        break
    elif index == 2:
        print("Skipping index 2")
        index += 1
        break

    print(l[index])
    index += 1