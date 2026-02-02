import pandas as pd

FILE_NAME = '...'
ROLE = 'smm'

def data_anonymization(df, role):
    if role.lower() == 'admin':
        return df

    df['card_number'] = '****'
    df['cvv'] = '****'

    return df

def read_transaction_data(file_name, role):
    df = pd.read_csv(file_name)

    anonym_df = data_anonymization(df, role)

    return anonym_df


if __name__ == '__main__':
    df = read_transaction_data(FILE_NAME, ROLE)

    print(df.sample(10, replace=True))

    a = 1 + 1