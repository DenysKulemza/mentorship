
def rec(n, iteration=1):
    print(f'Iteration: {iteration}, n = {n}')

    if n == 0:
        return 'finish recursion'

    return rec(n=n - 2, iteration=iteration + 1)

# rec -> function
# n -> argument
# n-2 -> operation on argument


if __name__ == '__main__':
    print(
        rec(n=10)
    )