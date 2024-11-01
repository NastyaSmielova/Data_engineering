import numpy as np


def create2D(size=(6, 6)):
    return np.random.randint(100, size=size)


def print_array(arr, message=None):
    if message:
        print(message)
    print(arr)


def save_array(arr, saving_format='csv'):
    print(f"saving into {saving_format}")
    if saving_format == "txt":
        np.savetxt("array.txt", arr)
    elif saving_format == "csv":
        np.savetxt("array.csv", arr, delimiter=",")
    elif saving_format == "npy":
        np.save("array.npy", arr)
    elif saving_format == "npz":
        np.savez("array.npz", arr)
    else:
        print("unknown format")


def get_sum(arr, axis=None):
    return np.sum(arr, axis=axis)


def get_mean(arr, axis=None):
    return np.mean(arr, axis=axis)


def get_median(arr, axis=None):
    return np.median(arr, axis=axis)


def get_std(arr, axis=None):
    return np.std(arr, axis=axis)


def load_from(load_format):
    print(f"loading from {load_format}")
    if load_format == "txt":
        return np.loadtxt("array.txt").astype(int)
    elif load_format == "csv":
        return np.loadtxt("array.csv", delimiter=",").astype(int)
    elif load_format == "npy":
        return np.load("array.npy").astype(int)
    elif load_format == "npz":
        arr = np.load("array.npz")
        data = arr['arr_0']
        arr.close()
        return data
    else:
        print("unknown format")


def check_savings():
    print("\tChecking different saving formats")
    saving_formats = ['txt', 'csv', 'npz', 'npy']
    for save_in in saving_formats:
        arr_2dim = create2D((10, 10))
        save_array(arr_2dim, save_in)
        read = load_from(save_in)
        if np.allclose(read, arr_2dim):
            print(f"saved-loaded is the same as original for {save_in} format")
        print_array(arr_2dim, "Original array")
        print_array(read, "Write-read array")


def use_aggregation_functions():
    arr_2dim = create2D((10, 5))
    print_array(arr_2dim, "Array for aggregation")
    print(f"array shapes: {arr_2dim.shape}")

    print("\tAggregate Functions")
    print("on whole matrix")

    print(f"sum: {get_sum(arr_2dim)}, mean: {get_mean(arr_2dim)}, "
          f"median: {get_median(arr_2dim)}, std: {get_std(arr_2dim)}")

    print("on columns")
    print(f"sum: {get_sum(arr_2dim, axis=0)},\nmean: {get_mean(arr_2dim, axis=0)},\n"
          f"median: {get_median(arr_2dim, axis=0)}, \nstd: {get_std(arr_2dim, axis=0)}")

    print("on rows")
    print(f"sum: {get_sum(arr_2dim, axis=1)}, \nmean: {get_mean(arr_2dim, axis=1)}, \n"
          f"median: {get_median(arr_2dim, axis=1)}, \nstd: {get_std(arr_2dim, axis=1)}")


def main():

    arr_2dim = create2D((10, 10))
    print("\tArray creation")
    print_array(arr_2dim, "2 dimensional array: ")

    check_savings()
    use_aggregation_functions()


if __name__ == '__main__':
    main()