import sys

# --- data structures

l = list() # -- []
s = set() # -- {}
t = tuple() # -- ()
d = dict() # -- {}


def multiple_return():
    return ('David', 205)


# -- iterators
# __iter__()
# __next__()

class CustomIterator:
    def __init__(self, string_numbers):
        self.string_numbers = string_numbers
        self.separated_string_numbers = string_numbers.split(',')
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        try:
            number = self.separated_string_numbers[self.index]
        except IndexError:
            raise StopIteration()
        self.index += 1
        return number

# -- generators

def gen_list(numbers):
    yield from numbers

def generators_array():
    for i in range(10):
        yield i

if __name__ == '__main__':
    lt = [1, 2, 4, 5, 5] # list
    st = {1, 2, 4, 5, 5} # set
    dt = {1: 'one', 2: 'two'} # dict
    tp = (1, 2, 3, 4, 5, 5) # tuple

    # for item in generators_array():
    #     print(item)

    # numbers_big_list = list(range(10000000))
    # gen_big_numbers = gen_list(numbers_big_list)
    #
    # numbers_small_list = list(range(100))
    # gen_small_numbers = gen_list(numbers_small_list)
    #
    # print('Big List size: ', sys.getsizeof(gen_big_numbers))
    # print('Small List size: ', sys.getsizeof(gen_small_numbers))


    # numbers = CustomIterator('1,2,3,4,5')
    #
    # for i in numbers:
    #     print(i)

    # res = multiple_return()
    #
    # print(type(res))

    # for item in tp:
    #     print(item)

    # for key, value in dt.items():
    #     print('Key: ', key, ' Value: ', value)