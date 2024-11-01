import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)


def categorize_availability(row):
    if row['availability_365'] < 50:
        return '"Rarely Available'
    if row['availability_365'] >= 200:
        return 'Highly Available'
    return 'Occasionally Available'


def describe(df, stats):
    d = df.describe().loc[['mean', 'std']]
    return d._append(df.reindex(d.columns, axis=1).agg(stats))


def print_analysis_results(df):
    pivoted = df.pivot_table(values="price", columns=["neighbourhood_group", 'room_type'])
    print("Pivoted:\n", pivoted, "\n")

    details_price = pd.melt(df, value_vars=['price', 'minimum_nights'], )
    print("Melt:\n", details_price, "\n")

    df['availability_status'] = df.apply(categorize_availability, axis=1).astype("category")

    correlations = df[['availability_status', 'price', 'number_of_reviews', 'neighbourhood_group']].apply(
        lambda x: pd.factorize(x)[0]).corr(method='pearson', min_periods=1)
    print("Correlations:\n", correlations, "\n")

    temp = df[['price', 'minimum_nights', 'number_of_reviews']]

    print("Descriptive Statistics: \n", describe(temp, ['median']),"\n")

    # Time Series Analysis:
    df['last_review'] = pd.to_datetime(df['last_review'])
    df.dropna(subset=['last_review'], inplace=True)
    df['year'] = df['last_review'].dt.year
    df['month'] = df['last_review'].dt.month
    df.set_index('last_review', inplace=True)
    stats = df.groupby('month').agg(price=('price', np.average),
                                    minimum_nights=('minimum_nights', np.average))
    print("Statistic monthly based:\n", stats)


def main():
    df = pd.read_csv("cleaned_airbnb_data.csv")
    print_analysis_results(df)
    df.to_csv("time_series_airbnb_data.csv")


if __name__ == '__main__':
    main()
