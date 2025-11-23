# --- loops
arr = [1, 2, 434, 5]

# for i in arr:
#     print(i)


# --- magic methods

a = 1 + 2
# a = 1.__add(2)__

# --- methods
def multiply_array():
    new_arr = []

    for i in arr:
        new_arr.append(i * 2)

    return new_arr

def multipy_array_args(array, multiplier):
    new_arr = []

    for i in array:
        new_arr.append(i * multiplier)

    return new_arr

def multipy_array_optional(array, multiplier=2):
    new_arr = []

    for i in array:
        new_arr.append(i * multiplier)

    return new_arr

# --- decorators
def array_length(func):
    def wrapper(*args, **kwargs):
        print('Array length: ', len(func(args, kwargs)))
        return func(args, kwargs)
    return wrapper

@array_length
def array_test(array, multiplier=2):
    new_arr = []

    for i in array:
        new_arr.append(i * multiplier)

    return new_arr

def args_tst(*args):
    print(args)

def kwargs_tst(**kwargs):
    print(kwargs)

def kwargs_args_tst(*args, **kwargs):
    print(args)
    print(kwargs)

if __name__ == '__main__':
    # result_function = multiply_array()

    # print(result_function)

    kwargs_args_tst(123414, 450, day=10, number=124312, str='asfsdfdsf')

    # new_arra = array_test(array=arr, multiplier=10)
    #
    # print(new_arra)

    # for index in range(0, len(arr) + 1):
    #     print('Index: ', index, ' array element: ', arr[index])
    #
    # index = 20
    #
    # while index > 0:
    #     print('Inside while loop: ', index)
    #
    #     index -= 1
    # index = index - 1
    #
    # api_call = 1
    #
    #
    # while True:
    #
    #     if api_call == 1:
    #         print('API response: ', api_call)
    #         # write to database
    #         api_call = 0
    #
    #     else:
    #         break
    #
    # print('Exit while  loop')

# range(arg1, arg2, arg3) -> arg1 = starting number,  arg2 = last number, arg3 = step

