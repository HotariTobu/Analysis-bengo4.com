# %% [markdown]
# ライブラリのインポート

# %%
import spacy
import ginza

import pandas as pd

# %% [markdown]
# データの読み込み

# %%
qus = pd.read_csv('src_ex/qus.csv', encoding="utf-8")
ans = pd.read_csv('src_ex/ans.csv', encoding="utf-8")

# %% [markdown]
# 関数定義

# %%
def loadlist(input_path):
    with open(input_path, 'r', encoding="utf-8") as f:
        return [line.strip() for line in f.readlines()]

# %%
PRE_BLACKLIST = loadlist('PRE_BLACKLIST.txt')
BLACKLIST = loadlist('BLACKLIST.txt')

nlp = spacy.load('ja_ginza')
ginza.set_split_mode(nlp, 'C')

def extract(text):
    if not isinstance(text, str):
        return []

    doc = nlp(text)
    terms = []

    def add(term):
        if term == '':
            return

        if term in BLACKLIST:
            return

        terms.append(term)

    for sent in doc.sents:
        term = ''

        for token in sent:
            if token.lemma_ in PRE_BLACKLIST:
                add(term)
                term = ''
                continue

            if token.pos_ in ['NOUN', 'PROPN', 'NUM']:
                term += token.lemma_
            else:
                add(term)
                term = ''

            if token.pos_ in ['ADJ', 'ADV', 'VERB']:
                add(token.lemma_)

        add(term)
            
    return terms

# %%
def save_lists(index, lists, output_path):
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

# %% [markdown]
# 用語の抜き出し

# %%
qus_terms = qus#.sample(100)
ans_terms = ans#.sample(100)

qus_terms['表題'] = qus_terms['表題'].apply(extract)
qus_terms['質問本文'] = qus_terms['質問本文'].apply(extract)
ans_terms['回答本文'] = ans_terms['回答本文'].apply(extract)

# %% [markdown]
# 保存

# %%
save_lists(qus_terms['質問ID'], qus_terms['表題'], 'src_ex/qus_title_terms.csv')
save_lists(qus_terms['質問ID'], qus_terms['質問本文'], 'src_ex/qus_body_terms.csv')
save_lists(ans_terms['回答ID'], ans_terms['回答本文'], 'src_ex/ans_body_terms.csv')

print('Completed!')