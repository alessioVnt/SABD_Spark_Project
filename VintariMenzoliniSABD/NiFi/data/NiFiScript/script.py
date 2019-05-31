import pandas as pd
import numpy as np
import math
import sys

#Upper limits
# temperature: 350
# pressure: 1200
# humidity: 100

upper_limit = int(sys.argv[1])


def clean_missing_data(df):
    list_columns = list(df.columns)
    list_columns.remove("datetime")

    for col in list_columns:
        df[col] = (df[col].ffill() + df[col].bfill()) / 2
        df[col] = df[col].bfill().ffill()

    return df


def check_value(x):
    if np.isnan(x):
        return np.NAN
    else:

        if x > upper_limit:
            if x >= 1000:
                digits = int(math.log10(x)) + 1
                diff = digits - 3
                res = x * pow(10, -diff)
                return res
            else:
                return np.NAN

    return x


def main():
    file = sys.stdin
    raw_data = pd.read_csv(file)

    columns = list(raw_data.columns)
    columns.remove("datetime")

    for col in columns:
        raw_data[col] = raw_data[col].apply(lambda x: check_value(x))

    processed_data = clean_missing_data(raw_data)

    sys.stdout.write(processed_data.to_csv(index=None, header=True))

    return 0


if __name__ == '__main__':
    main()
