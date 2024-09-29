import numpy as np
import datetime;


def get_revenue(data):
    return (data['quantity'] * data['price']).sum()


def get_num_unique_users(data):
    return np.unique(data['user_id']).size


def cast_price(data):
    print_types(data)
    data['price'] = data['price'].astype(int)
    return data


def print_types(data):
    print(data.dtype)


def most_purchased(data):
    unique_products = np.unique(data['product_id'])
    print(len(unique_products))
    print(unique_products)

    summary = {}
    for product in unique_products:
        summary[int(product)] = int(data[data['product_id'] == product]['quantity'].sum())
    print("Details about purchases: ", summary)
    summary = dict(sorted(summary.items(), key=lambda item: item[1], reverse=True))
    return list(summary.keys())[0]


def transactions_per_user(data):
    unique_users = np.unique(data['user_id'])
    print(len(unique_users))
    print(unique_users)

    summary = {}
    for user in unique_users:
        summary[int(user)] = int(data[data['user_id'] == user].size)
    print("Details about users: ", summary)
    summary = dict(sorted(summary.items(), key=lambda item: item[1], reverse=True))


def slice(dataframe, columns=['product_id', 'quantity']):
    return dataframe[columns]


def price_increase(data, percent=5):
    data['price'] = data['price'] * (100 + percent)/100
    return data


def generate_data(num_of_logs=20):
    data = {}
    type_formats = {}
    np.random.seed(17)
    data['transaction_id'] = np.random.choice(np.arange(100, 200), num_of_logs, replace=False)
    type_formats['transaction_id'] = int
    data['user_id'] = np.random.choice(np.arange(200, 220), num_of_logs, replace=True)
    type_formats['user_id'] = int
    data['product_id'] = np.random.choice(np.arange(500, 600), num_of_logs, replace=True)
    type_formats['product_id'] = int
    data['quantity'] = np.random.choice(np.arange(0, 10), num_of_logs, replace=True)
    type_formats['quantity'] = int
    data['price'] = np.random.uniform(1, 100, num_of_logs).round(2)
    type_formats['price'] = float
    data['timestamp'] = [np.datetime64(datetime.datetime.now() - datetime.timedelta(np.random.randint(0, 20))) for _ in range(num_of_logs)]
    type_formats['timestamp'] = datetime.datetime

    dataframe = np.zeros(num_of_logs, dtype=(list(type_formats.items())))
    for k, v in data.items():
        dataframe[k] = v

    return dataframe


def generate_masked_array(data, limit=0):
    mask = data['quantity'] > limit
    masked = data[mask]
    return masked


def user_transactions(data, user_id):
    mask = data['user_id'] == user_id
    masked = data[mask]
    return masked


def get_log_for_time(data, time):
    mask = data['timestamp'] >= time[0]
    mask_2 = data['timestamp'] <= time[1]
    mask = mask_2 * mask
    masked = data[mask]
    return masked


def compare_revenue(data, time_1, time_2):
    data_1 = get_log_for_time(data, time_1)
    data_2 = get_log_for_time(data, time_2)
    r_1 = get_revenue(data_1)
    r_2 = get_revenue(data_2)
    print(f"Revenue {r_1}, revenue {r_2}")


def top_products_transactions(data):
    unique_products = np.unique(data['product_id'])
    summary = {}
    for product in unique_products:
        summary[int(product)] = get_revenue(data[data['product_id'] == product])
    summary = dict(sorted(summary.items(), key=lambda item: item[1], reverse=True))

    products_ids = list(summary.keys())[:5]
    mask = [True if log['product_id'] in products_ids else False for log in data]
    return data[mask]


def main():
    num_of_logs = 20
    dataframe = generate_data(num_of_logs)
    print("initial database: \n", dataframe)

    print("Total revenue: ", get_revenue(dataframe))
    print("Number of unique users: ", get_num_unique_users(dataframe))
    print("Most purchased product: ", most_purchased(dataframe))

    print("Casting price:\n", cast_price(dataframe))

    print("Product_id + quantity:", slice(dataframe))
    print("Transactions per user: ", transactions_per_user(dataframe))

    filtered_arr = generate_masked_array(dataframe)
    print(f"Previously: {dataframe.size}, quantity > 0 condition: {filtered_arr.size}")

    print("Increased price:\n", price_increase(dataframe))
    filtered_arr = generate_masked_array(dataframe, 1)
    print(f"Previously: {dataframe.size}, quantity > 0 condition: {filtered_arr.size}")

    times = dataframe['timestamp']
    first_period = [times[4], times[5]]
    second_period = [times[9], times[10]]
    compare_revenue(dataframe, first_period, second_period)

    user = dataframe['user_id'][0]
    print(f"User transactions: {user} \n", user_transactions(dataframe, user))
    print(f"Transactions for period: {first_period}\n", get_log_for_time(dataframe, first_period))

    print("Top 5 product's transactions\n", top_products_transactions(dataframe))


if __name__ == '__main__':
    main()