# %% [markdown]
# 必要なライブラリのインポート

# %%
import math

import pandas as pd
from multiprocessing import Pool

def f(x):
    return x*x

if __name__ == '__main__':
    with Pool(5) as p:
        print(p.map(f, [1, 2, 3]))

import save

def load():
    qus = pd.read_csv('src_ex/qus.csv')
    ans = pd.read_csv('src_ex/ans.csv')
    
    qus_title_terms = pd.read_csv('src_ex/qus_title_terms.csv')
    qus_body_terms = pd.read_csv('src_ex/qus_body_terms.csv')
    ans_body_terms = pd.read_csv('src_ex/ans_body_terms.csv')

    qus_title_terms = qus_title_terms.groupby('質問ID').agg(list).reset_index()
    qus_body_terms = qus_body_terms.groupby('質問ID').agg(list).reset_index()
    ans_body_terms = ans_body_terms.groupby('回答ID').agg(list).reset_index()
    
    qus_terms = qus.drop(['表題', '質問本文'], axis='columns').merge(qus_title_terms)
    qus_terms = qus_terms.merge(qus_body_terms)
    ans_terms = ans.drop('回答本文', axis='columns').merge(ans_body_terms)
    pass

# %% [markdown]
# データの読み込み

# %%
qus = pd.read_csv('src_ex/qus.csv')
ans = pd.read_csv('src_ex/ans.csv')

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
qus_terms = qus.drop(['表題', '質問本文'], axis='columns').merge(qus_title_terms)
qus_terms = qus_terms.merge(qus_body_terms)
ans_terms = ans.drop('回答本文', axis='columns').merge(ans_body_terms)

# %% [markdown]
# ## TFIDF

# %% [markdown]
# ### 前準備

# %%
def tf(term_lists):
    all_terms = sum(term_lists, [])
    term_counts = {}

    for term in all_terms:
        if term in term_counts:
            term_counts[term] += 1
        else:
            term_counts[term] = 1

    all_terms_count = len(all_terms)
    tf_dict = {}

    for term in term_counts.keys():
        tf = term_counts[term] / all_terms_count
        
        tf_dict[term] = tf

    return tf_dict

# %%
def tfidf(term_lists):
    def list_count_of(term):
        count = 0

        for term_list in term_lists:
            if term in term_list:
                count += 1

        return count
    
    tf_dict = tf(term_lists)

    term_list_count = len(term_lists)
    tfidf_dict = {}

    for term in tf_dict.keys():
        list_count = list_count_of(term)
        idf = -math.log(list_count / term_list_count)
        
        tfidf_dict[term] = tf_dict[term] * idf

    return tf_dict, tfidf_dict

# %% [markdown]
# ### カテゴリごと

# %% [markdown]
# #### 計算

# %%
ans_body_by_question_id = ans_terms.groupby('質問ID').agg({'回答本文': sum}, start=[]).reset_index()
ans_qus = qus_terms.merge(ans_body_by_question_id)[['カテゴリ名', '表題', '質問本文', '回答本文']]
terms_by_question_id = ans_qus['表題'] + ans_qus['質問本文'] + ans_qus['回答本文']
cas_terms_by_question_id = pd.DataFrame({
    'カテゴリ名': ans_qus['カテゴリ名'],
    '用語': terms_by_question_id,
})

# %%
cas_terms = cas_terms_by_question_id.groupby('カテゴリ名').agg(list)

# %%
cas_tfidf = pd.DataFrame(
    index=cas_terms.index,
    columns={
        'TFIDF': [tfidf(term_lists) for term_lists in cas_terms['用語']]
    }
)

# %% [markdown]
# #### 保存

# %%
for category_name, term_lists in cas_terms.itertuples():
    save.lists('文章ID', term_lists, f"dst/terms/{category_name}.csv")

for category_name, (tfs, tfidfs) in cas_tfidf.itertuples():
    save.dict(tfs, '用語', 'TF', f"dst/tf/{category_name}.csv")
    save.dict(tfidfs, '用語', 'TFIDF', f"dst/tfidf/{category_name}.csv")

# %% [markdown]
# ### 文章の種類ごと

# %% [markdown]
# #### 計算

# %%
qus_title_terms = qus_terms['表題']
qus_title_tf, qus_title_tfidf = tfidf(qus_title_terms)

# %%
qus_body_terms = qus_terms['質問本文']
qus_body_tf, qus_body_tfidf = tfidf(qus_body_terms)

# %%
ans_body_terms = ans_terms['回答本文']
ans_body_tf, ans_body_tfidf = tfidf(ans_body_terms)

# %%
all_terms = pd.concat([qus_title_terms, qus_body_terms, ans_body_terms])
all_tf, all_tfidf = tfidf(all_terms)

# %% [markdown]
# #### 保存

# %%
save.dict(qus_title_tf, '用語', 'TF', 'dst/tf/qus_title_tf.csv')
save.dict(qus_body_tf, '用語', 'TF', 'dst/tf/qus_body_tf.csv')
save.dict(ans_body_tf, '用語', 'TF', 'dst/tf/and_body_tf.csv')
save.dict(all_tf, '用語', 'TF', 'dst/tf/all_tf.csv')

# %%
save.dict(qus_title_tfidf, '用語', 'TFIDF', 'dst/tfidf/qus_title_tfidf.csv')
save.dict(qus_body_tfidf, '用語', 'TFIDF', 'dst/tfidf/qus_body_tfidf.csv')
save.dict(ans_body_tfidf, '用語', 'TFIDF', 'dst/tfidf/and_body_tfidf.csv')
save.dict(all_tfidf, '用語', 'TFIDF', 'dst/tfidf/all_tfidf.csv')
