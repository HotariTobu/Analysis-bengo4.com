import pandas as pd

def lists(index, lists, output_path):
    sub_dfs = []

    if isinstance(index, str):
        index_type = int
        index_name = index
        indexed_lists = enumerate(lists)
    else:
        index_type = index.dtype
        index_name = index.name
        indexed_lists = zip(index, lists)

    list_label = lists.name
    
    for i, list in indexed_lists:
        sub_index = pd.Series([i] * len(list), dtype=index_type, name=index_name)
        sub_df = pd.DataFrame(
            index=sub_index,
            data={
                list_label: list,
            }
        )
        sub_dfs.append(sub_df)
        
    df = pd.concat(sub_dfs)
    df.to_csv(output_path)

def dict(dict_data, key_name, value_name, output_path):
    df = pd.DataFrame(
        index=pd.Series(dict_data.keys(), name=key_name),
        columns={
            value_name: dict_data.values(),
        }
    )
    df.to_csv(output_path)