# %% [markdown]
# 必要なライブラリのインポート

# %%
import pandas as pd

from itertools import combinations

# %% [markdown]
# データの読み込み

# %%
qus_title_terms = pd.read_csv('src_ex/qus_title_terms.csv')
qus_body_terms = pd.read_csv('src_ex/qus_body_terms.csv')
ans_body_terms = pd.read_csv('src_ex/ans_body_terms.csv')

# %% [markdown]
# 用語の復元

# %%
qus_title_terms = qus_title_terms.groupby('質問ID').agg(list).reset_index()
qus_body_terms = qus_body_terms.groupby('質問ID').agg(list).reset_index()
ans_body_terms = ans_body_terms.groupby('回答ID').agg(list).reset_index()

# %%
#########################################################################################################################################
def a():
    qus = pd.read_csv('src_ex/qus.csv')
    ans = pd.read_csv('src_ex/ans.csv')
    qus_terms = qus.drop(['表題', '質問本文'], axis='columns').merge(qus_title_terms)
    qus_terms = qus_terms.merge(qus_body_terms)
    ans_terms = ans.drop('回答本文', axis='columns').merge(ans_body_terms)
    ans_body_by_question_id = ans_terms.groupby('質問ID').agg({'回答本文': sum}, start=[]).reset_index()
    ans_qus = qus_terms.merge(ans_body_by_question_id)[['カテゴリ名', '表題', '質問本文', '回答本文']]
    terms_by_question_id = ans_qus['表題'] + ans_qus['質問本文'] + ans_qus['回答本文']
    cas_terms_by_question_id = pd.DataFrame({
        'カテゴリ名': ans_qus['カテゴリ名'],
        '用語': terms_by_question_id,
    })
    return cas_terms_by_question_id.groupby('カテゴリ名').agg(list)
cas_terms = a()
#########################################################################################################################################

# %%
qus_title_terms = qus_title_terms['表題']
qus_body_terms = qus_body_terms['質問本文']
ans_body_terms = ans_body_terms['回答本文']
all_terms = pd.concat([qus_title_terms, qus_body_terms, ans_body_terms])

# %%
def jaccard(term_lists):
    def add_counts(counts, values):
        for value in values:
            if value in counts:
                counts[value] += 1
            else:
                counts[value] = 1

    term_counts = {}
    pair_counts = {}

    for term_list in term_lists:
        terms_set = set(term_list)

        add_counts(term_counts, terms_set)

        sub_pairs = combinations(terms_set, 2)
        sub_pairs = [tuple(sorted(pair)) for pair in sub_pairs]
        add_counts(pair_counts, sub_pairs)

    jaccard_dict = {}

    for pair, count in pair_counts.items():
        coef = count / (term_counts[pair[0]] + term_counts[pair[1]] - count)
        jaccard_dict[pair] = (count, coef)
    
    return jaccard_dict

# %%
def save_jaccard(jaccard_dict, output_path):
    data = [[*key, *value] for key, value in jaccard_dict.items()]
    df = pd.DataFrame(data, columns=['用語1', '用語2', '出現回数', 'Jaccard'])
    df.set_index(['用語1', '用語2'], inplace=True)
    df.sort_values('Jaccard', ascending=False, inplace=True)
    df.to_csv(output_path)

# %%
for category_name, term_lists in cas_terms.itertuples():
    pass
    save_jaccard(jaccard(term_lists), f"dst/jaccard/{category_name}.csv")

# %%
save_jaccard(jaccard(qus_title_terms), 'dst/jaccard/qus_title_terms.csv')
save_jaccard(jaccard(qus_body_terms), 'dst/jaccard/qus_body_terms.csv')
save_jaccard(jaccard(ans_body_terms), 'dst/jaccard/ans_body_terms.csv')
save_jaccard(jaccard(all_terms), 'dst/jaccard/all_terms.csv')
