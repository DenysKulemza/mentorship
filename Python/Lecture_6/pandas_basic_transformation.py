import pandas as pd
import json

COLUMNS = ['item_sku', 'snapshot_date_y', 'current_stock_y', 'warehouse_id_y']

RENAMED_COLUMNS = {'snapshot_date_y': 'snapshot_date', 'current_stock_y': 'current_stock', 'warehouse_id_y': 'warehouse_id'}

if __name__ == '__main__':

    with open('inventory_updates.json', 'r') as f:
        data = json.load(f)

    df_js = pd.json_normalize(data)

    warehouse_d_df = df_js[df_js['warehouse_id'] == 'D']

    warehouse_a_df = df_js[df_js['warehouse_id'] == 'A']

    # merged_df = warehouse_d_df.merge(warehouse_a_df, how='inner', on='item_sku')

    left_join = pd.merge(warehouse_a_df, warehouse_d_df, how='outer', on='item_sku', indicator=True)

    # final_df = left_join[left_join['item_sku'] == 'PROD-085']

    final_df = left_join[left_join['_merge'] == 'right_only']

    final_df = final_df[COLUMNS]

    final_df = final_df.rename(columns=RENAMED_COLUMNS)

    print(final_df.sample(replace=True))
    print(final_df.columns)

    final_df.to_csv('result.csv', index=False)

    # ['item_sku', 'snapshot_date_y', 'current_stock_y', 'warehouse_id_y',
    #  'snapshot_date_y', 'current_stock_y', 'warehouse_id_y', '_merge']

    # unioned_df = pd.concat([warehouse_a_df, warehouse_d_df], axis=1)

    # print(unioned_df.sample(n=5))
    # print(final_df.columns)
