import numpy as np


def create2D(size=(6, 6)):
    return np.random.randint(100, size=size)


def print_array(arr, message=None):
    if message:
        print(message)
    print(arr)


def transpose(arr):
    return np.transpose(arr)


def reshape(arr, new_shape=(3, 12)):
    return arr.reshape(new_shape)


def split(arr):
    return np.split(arr, indices_or_sections=len(arr),  axis=0)


def combine(arr1, arr2):
    return np.concatenate((arr1, arr1), axis=0)


def main():

    arr_2dim = create2D()
    print("\tArray creation")
    print_array(arr_2dim, "2 dimensional array: ")

    print("\tArray Manipulation Functions")
    print_array(transpose(arr_2dim), "Transpose: ")
    print_array(reshape(arr_2dim), "Reshape: ")

    print_array(split(arr_2dim), "Split: ")
    print_array(combine(arr_2dim, create2D()), "Combine: ")


if __name__ == '__main__':
    main()