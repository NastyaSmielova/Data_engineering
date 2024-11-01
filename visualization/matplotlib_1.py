import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

pd.set_option('display.max_columns', None)


def neighborhood_distribution_of_listings(data):
    fig, ax = plt.subplots(figsize=(14, 7))

    color_list = ['b', 'g', 'r', 'c', 'm']

    ax.bar(data['neighbourhood_group'], data['listing'], width=0.2,
           color=color_list, label=data['neighbourhood_group'])

    ax.set_title('Neighborhood Distribution of Listings',
                 loc='left')
    for index, row in data.iterrows():
        plt.text(index - 0.1, row['listing'] + 0.2, str(row['listing']))

    plt.xlabel('neighbourhood')
    plt.ylabel('Listing number')
    plt.legend()
    plt.savefig("Neighborhood Distribution of Listings.png")
    plt.show()
    plt.close(fig)


def price_distribution(grouped):
    fig, ax = plt.subplots(figsize=(14, 7))
    prices = []
    names = []
    for name, group in grouped:
        prices.append(group['price'])
        names.append(name[0])
    print(names)
    plt.boxplot(prices, labels=names)
    ax.set_title('Price Distribution Across Neighborhoods',
                 loc='left')
    plt.xlabel('neighbourhood')
    plt.ylabel('price')
    plt.savefig("Price Distribution Across Neighborhoods.png")
    plt.show()
    plt.close(fig)


def availability_plot(data):

    df_pivot = pd.pivot_table(
        data,
        values="availability_365",
        index="room_type",
        columns="neighbourhood_group",
        aggfunc=np.average
    )

    ax = df_pivot.plot(kind="bar")
    fig = ax.get_figure()

    ax.set_title('Room Type vs. Availability',
                 loc='left')
    plt.xticks(rotation=0)

    plt.xlabel('Room type')
    plt.ylabel('average availability_365')
    plt.savefig("Room Type vs. Availability.png")
    plt.show()
    plt.close(fig)


def price_nums_review_plot(data):
    fig, ax = plt.subplots(figsize=(14, 7))
    color_list = ['b', 'g', 'r', 'c', 'm']
    i = 0
    patches = []
    for type in data['room_type'].unique():
        res = data[data['room_type'] == type]
        m, b = np.polyfit(res['price'], res['number_of_reviews'], 1)
        plt.plot(res['price'], m * res['price'] + b, c=color_list[i])
        patch = mpatches.Patch(color=color_list[i], label=type)
        patches.append(patch)
        plt.scatter(res['price'], res['number_of_reviews'], c=color_list[i], label=type)
        i += 1
    ax.set_title('Correlation Between Price and Number of Reviews',
                 loc='left')
    plt.xlabel('price')
    plt.ylabel('number of reviews')
    plt.legend(handles=patches)
    plt.savefig("Correlation Between Price and Number of Reviews.png")

    plt.show()
    plt.close(fig)

def main():
    data = pd.read_csv("cleaned_airbnb_data.csv")
    neighborhood_distribution = (data.groupby(['neighbourhood_group'], as_index=False)
                                 .agg(listing=('id', np.count_nonzero)))
    print(neighborhood_distribution)
    neighborhood_distribution_of_listings(neighborhood_distribution)
    prices = data[['price', 'neighbourhood_group']].groupby(['neighbourhood_group'], as_index=False)
    price_distribution(prices)

    availability = (data[['availability_365', 'room_type',
                         'neighbourhood_group']].groupby(['neighbourhood_group', 'room_type'],
                                                         as_index=False).
                    agg(average_availability_365=('availability_365', np.average),
                        std_availability_365=('availability_365', np.std)))
    availability_plot(data)

    price_nums_review = data[['price', 'number_of_reviews', 'room_type']]
    price_nums_review_plot(price_nums_review)


if __name__ == '__main__':
    main()

