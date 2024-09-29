import pandas as pd
import numpy as np
from bokeh.plotting import figure, show, output_file, save
from bokeh.models import ColumnDataSource, FactorRange
from bokeh.palettes import Bright6
import warnings

np.set_printoptions(suppress=True)
warnings.simplefilter(action='ignore', category=FutureWarning)


def categorize_age(row):
    # Child, Young Adult, Adult, Senior
    if row['Age'] <= 17:
        return 'Child'
    if 18 <= row['Age'] <= 24:
        return 'Young Adult'
    if 25 <= row['Age'] <= 60:
        return 'Adult'
    return 'Senior'


def calc_survival(row, counts):
    return counts[counts['AgeGroup'] == row['AgeGroup']].SurvivalRate.values[0]


def plot_survival_rates(counts):
    output_file(filename="Age Group Survival.html", title="Static HTML file")
    source = ColumnDataSource(data=dict(age=counts['AgeGroup'].to_list(),
                                        counts=counts['Survived'].to_list(),
                                        rate=counts['SurvivalRate'].to_list(),
                                        color=Bright6))
    TOOLTIPS = [
        ("Age Group", "@age"),
        ("Total amount", "@counts"),
        ("Survival Rate", "@rate"),
    ]
    p = figure(x_range=counts['AgeGroup'].to_list(), height=350, title="Age Group Survival",
               toolbar_location="below", tooltips=TOOLTIPS,
               toolbar_sticky=False)

    p.vbar(x='age', top='rate', width=0.9, color='color', legend_field="age", source=source)

    p.xgrid.grid_line_color = None
    p.y_range.start = 0

    show(p)
    save(p)


def plot_class_gender(df):
    output_file(filename="Class and Gender.html", title="Static HTML file")

    x = [(sex, str(class_)) for sex in df['Sex'].unique() for class_ in df['Pclass'].unique()]

    arr = [df[(df['Sex'] == sex) & (df['Pclass'] == int(class_))].SurvivalRate.mean()
           for (sex, class_) in x]

    counts = sum(zip(arr), ())
    tooltips = [
        ("Sex, class", "@x"),
        ("SurvivalRate", "@counts"),
    ]
    source = ColumnDataSource(data=dict(x=x, counts=counts))

    p = figure(x_range=FactorRange(*x), height=350, title="Class and Gender",
               toolbar_location="below", tooltips=tooltips,
               toolbar_sticky=False)

    p.vbar(x='x', top='counts', width=0.9, source=source)

    p.y_range.start = 0
    p.x_range.range_padding = 0.1
    p.xaxis.major_label_orientation = 1
    p.xgrid.grid_line_color = None

    show(p)
    save(p)


def fave_survival(df):
    output_file(filename="Fare vs. Survival.html", title="Static HTML file")

    p = figure(title="Fare vs. Survival",
               toolbar_location="below",
               toolbar_sticky=False)
    color = ['red', 'green', 'blue', 'magenta', 'k']

    i = 0
    for class_ in df['Pclass'].unique():
        temp = df[df['Pclass'] == class_]
        p.circle(temp['Fare'], temp['Survived'], size=10, color=color[i], legend_label=str(class_))
        i += 1
    show(p)
    save(p)


def main():
    df = pd.read_csv("Titanic-Dataset.csv")
    print(df)
    df['Age'] = df['Age'].fillna(df['Age'].mean())
    df['Embarked'] = df['Embarked'].fillna("Undefined")
    df['Cabin'] = df['Cabin'].fillna("Undefined")
    df['AgeGroup'] = df.apply(categorize_age, axis=1).astype("category")

    counts = df.groupby('AgeGroup', as_index=False).agg(Survived=('Survived', np.sum),
                                    total=('PassengerId', np.count_nonzero))

    counts['SurvivalRate'] = counts['Survived'] / counts['total']
    df['SurvivalRate'] = df.apply(lambda x: calc_survival(x, counts), axis=1)

    plot_survival_rates(counts)
    plot_class_gender(df)
    fave_survival(df)


if __name__ == '__main__':
    main()
