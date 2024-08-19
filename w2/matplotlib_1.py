import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)


def main():
    data = pd.read_csv("AB_NYC_2019.csv")
    res = data.groupby(['neighbourhood_group']).count()
    print(res)


if __name__ == '__main__':
    main()

