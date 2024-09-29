import numpy as np


def create1D(num_points=10):
    return np.arange(1, 1 + num_points)


def create2D(num_points=9):
    return np.arange(1, 1 + num_points).reshape(3, 3)


def print_array(arr, message=None):
    if message:
        print(message)
    print(arr)


def get_element_by_index(arr, index=3):
    if len(arr) > index:
        return arr[index]
    else:
        return None


def get_slice(arr, slices=[2, 2]):
    return arr[:slices[0], :slices[1]]


def add_elementwise(arr, element=5):
    return arr + element


def multiply_elementwise(arr, coef=2):
    return arr * coef


def main():

    arr_1dim = create1D()
    arr_2dim = create2D()

    print("\tArray creation")
    print_array(arr_1dim, "1 dimensional array: ")
    print_array(arr_2dim, "2 dimensional array: ")

    print("\tBasic operations")
    print_array(get_element_by_index(arr_1dim, 3), "The third element: ")
    print_array(get_slice(arr_2dim), "2x2 slice: ")

    print_array(add_elementwise(arr_1dim, 5), "Added 5 to each element: ")
    print_array(multiply_elementwise(arr_2dim, 2), "Multiplied by 2: ")


if __name__ == '__main__':
    main()