import pandas as pd

pd.set_option('display.max_columns', None)


def categorize_prise(row):
    if row['price'] < 100:
        return 'Low'
    if row['price'] >= 300:
        return 'High'
    return 'Medium'


def categorize_staying(row):
    if row['minimum_nights'] <= 3:
        return 'short-term'
    if row['minimum_nights'] > 14:
        return 'long-term'
    return 'medium-term'


class NY_Dataset:

    def __init__(self, filename):
        self.df = pd.read_csv(filename)

    def print_head(self):
        print(self.df.head(5))

    def print_info(self):
        print(self.df.info)

    def check_nan(self):
        have_nans = self.df.columns[self.df.isnull().any()].tolist()
        print(f"columns with nan values: \n{have_nans}")
        print(f"with amount of: \n{self.df[have_nans].isnull().sum()}")

    def handle_nans(self):
        self.df[['name', 'host_name']] = self.df[['name', 'host_name']].fillna(value='Unknown')
        self.df['last_review'] = self.df['last_review'].fillna(value='NaT')

    def add_category(self):
        self.df['price_category'] = self.df.apply(categorize_prise, axis=1).astype("category")

    def add_length_of_stay(self):
        self.df['length_of_stay_category'] = self.df.apply(categorize_staying, axis=1).astype("category")

    def validate_price(self, verbose=True):
        if verbose:
            print("Minimal price BEFORE validation: ", self.df['price'].min())
        self.df.drop(self.df[self.df['price'] <= 0].index, inplace=True)
        if verbose:
            print("Minimal price AFTER validation: ", self.df['price'].min())

    def save_into(self, name):
        self.df.to_csv(f"{name}.csv")

    def print_dataframe_info(self, message=None):
        if message:
            print(message)
            self.print_head()
            self.check_nan()


def main():
    data = NY_Dataset("AB_NYC_2019.csv")
    data.print_head()
    data.check_nan()
    data.handle_nans()
    data.add_category()
    data.add_length_of_stay()
    data.validate_price()
    data.save_into("cleaned_airbnb_data")
    data.print_dataframe_info("Dataset information")


if __name__ == '__main__':
    main()

