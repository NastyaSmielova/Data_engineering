import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)


def try_simple_selection(df):
    print(df.loc[5])
    print(df.iloc[:, [5]])
    print(df.loc[5, ['price']])
    print(df['price'])


def filter_by_min_price(df, price=100):
    return df[df['price'] > price]


def filter_by_min_reviews(df, reviews_num=10):
    return df[df['number_of_reviews'] > reviews_num]


def select_needed_columns(df, columns):
    return df[columns]


def print_grouped_data(df, text=None):
    if text:
        print(text)
    print(df)


def main():
    df = pd.read_csv("cleaned_airbnb_data.csv")
    # not saving, just trying selection
    try_simple_selection(df)
    print("Total size before filtering: ", df.size)
    df = filter_by_min_price(df, 100)
    print("Size after price filtering: ", df.size)
    df = filter_by_min_reviews(df, 10)
    print("Size after num reviews filtering: ", df.size)
    columns = ['neighbourhood_group', 'price', 'minimum_nights',
               'number_of_reviews', 'price_category', 'availability_365']
    interesting_values_df = select_needed_columns(df, columns)
    print(interesting_values_df)

    grouped_1 = interesting_values_df.groupby(['neighbourhood_group', 'price_category'])
    aggregation_1 = grouped_1.agg(price=('price', np.average),
                                  minimum_nights=('minimum_nights', np.average),
                                  number_of_reviews=('number_of_reviews', np.average),
                                  availability_365=('availability_365', np.average),
                                  total_listing=('price', np.count_nonzero))

    print(aggregation_1)

    sorted_aggregation = aggregation_1.sort_values(by=['price', 'number_of_reviews'],
                                                   ascending=[False, True])
    print(sorted_aggregation)

    # maybe ranking with grouped only on neighbourhood_group was meant to be here
    # grouped_2 = interesting_values_df.groupby(['neighbourhood_group'])
    # aggregation_2 = grouped_2.agg(price=('price', np.average),
    #                               total_listing=('name', np.count_nonzero))
    # print(aggregation_2)

    aggregation_1['rank'] = (aggregation_1[["price", "total_listing"]]
                             .apply(tuple, axis=1)
                             .rank(method='max', ascending=False)
                             .astype(int))
    print(aggregation_1)
    aggregation_1.to_csv(f"aggregated_airbnb_data.csv")


if __name__ == '__main__':
    main()
