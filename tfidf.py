# %% [markdown]
# 必要なライブラリのインポート

# %%
import math

import pandas as pd
from multiprocessing import Pool

import save

def enlist(df_group):
    return df_group[0].groupby(df_group[1]).agg(list).reset_index()

def load():
    qus = pd.read_csv('src_ex/qus.csv')
    ans = pd.read_csv('src_ex/ans.csv')
    
    qus_title_terms = pd.read_csv('src_ex/qus_title_terms.csv')
    qus_body_terms = pd.read_csv('src_ex/qus_body_terms.csv')
    ans_body_terms = pd.read_csv('src_ex/ans_body_terms.csv')

    with Pool(3) as p:
        (
            qus_title_terms,
            qus_body_terms,
            ans_body_terms
        ) = p.map(enlist,
            [
                (qus_title_terms, '質問ID'),
                (qus_body_terms, '質問ID'),
                (ans_body_terms, '回答ID'),
            ]
        )

    qus_terms = qus.drop(['表題', '質問本文'], axis='columns').merge(qus_title_terms)
    qus_terms = qus_terms.merge(qus_body_terms)
    ans_terms = ans.drop('回答本文', axis='columns').merge(ans_body_terms)
    
    return qus_terms, ans_terms

# # %% [markdown]
# # データの読み込み

# # %%
# qus = pd.read_csv('src_ex/qus.csv')
# ans = pd.read_csv('src_ex/ans.csv')

# # %%
# qus_title_terms = pd.read_csv('src_ex/qus_title_terms.csv')
# qus_body_terms = pd.read_csv('src_ex/qus_body_terms.csv')
# ans_body_terms = pd.read_csv('src_ex/ans_body_terms.csv')

# # %% [markdown]
# # 用語の復元

# # %%
# qus_title_terms = qus_title_terms.groupby('質問ID').agg(list).reset_index()
# qus_body_terms = qus_body_terms.groupby('質問ID').agg(list).reset_index()
# ans_body_terms = ans_body_terms.groupby('回答ID').agg(list).reset_index()

# # %%
# qus_terms = qus.drop(['表題', '質問本文'], axis='columns').merge(qus_title_terms)
# qus_terms = qus_terms.merge(qus_body_terms)
# ans_terms = ans.drop('回答本文', axis='columns').merge(ans_body_terms)

# # %% [markdown]
# # ## TFIDF

# # %% [markdown]
# # ### 前準備

# %%
def count_terms(term_list):
    term_counts = {}

    for term in term_list:
        if term in term_counts:
            term_counts[term] += 1
        else:
            term_counts[term] = 1
    
    return term_counts

def tf(term_lists):
    all_term_counts = {}
    all_terms_count = 0

    with Pool(8) as p:
        term_counts_list = p.map(count_terms, term_lists, chunksize=1024)
        for term_counts in term_counts_list:
            for term, count in term_counts.items():
                if term in all_term_counts:
                    all_term_counts[term] += count
                else:
                    all_term_counts[term] = count
                
                all_terms_count += count

    tf_dict = {}

    for term, count in all_term_counts.items():
        tf = count / all_terms_count
        tf_dict[term] = tf

    return tf_dict

# %%
def idf(term_list_count_all):
    return -math.log(term_list_count_all[0] / term_list_count_all[1])
    
def tfidf(term_lists):
    tf_dict = tf(term_lists)

    term_list_counts = {}
    all_term_list_count = len(term_lists)

    tfidf_dict = {}

    with Pool(8) as p:
        term_sets = p.map(set, term_lists, chunksize=1024)
        for term_set in term_sets:
            for term in term_set:
                if term in term_list_counts:
                    term_list_counts[term] += 1
                else:
                    term_list_counts[term] = 1
                    
        idf_gen = [(term_list_counts[term], all_term_list_count) for term in tf_dict]
        idfs = p.map(idf, idf_gen, chunksize=1024)
        tfidf_dict = {term: tf * idf for (term, tf), idf in zip(tf_dict.items(), idfs)}

    return tf_dict, tfidf_dict

# # %% [markdown]
# # ### カテゴリごと

# # %% [markdown]
# # #### 計算

# # %%
# ans_body_by_question_id = ans_terms.groupby('質問ID').agg({'回答本文': sum}, start=[]).reset_index()
# ans_qus = qus_terms.merge(ans_body_by_question_id)[['カテゴリ名', '表題', '質問本文', '回答本文']]
# terms_by_question_id = ans_qus['表題'] + ans_qus['質問本文'] + ans_qus['回答本文']
# cas_terms_by_question_id = pd.DataFrame({
#     'カテゴリ名': ans_qus['カテゴリ名'],
#     '用語': terms_by_question_id,
# })

# # %%
# cas_terms = cas_terms_by_question_id.groupby('カテゴリ名').agg(list)

# # %%
# cas_tfidf = pd.DataFrame(
#     index=cas_terms.index,
#     columns={
#         'TFIDF': [tfidf(term_lists) for term_lists in cas_terms['用語']]
#     }
# )

# # %% [markdown]
# # #### 保存

# # %%
# for category_name, term_lists in cas_terms.itertuples():
#     save.lists('文章ID', pd.Series(term_lists, name='用語'), f"dst/terms/{category_name}.csv")

# for category_name, (tfs, tfidfs) in cas_tfidf.itertuples():
#     save.dict(tfs, '用語', 'TF', f"dst/tf/{category_name}.csv")
#     save.dict(tfidfs, '用語', 'TFIDF', f"dst/tfidf/{category_name}.csv")

# # %% [markdown]
# # ### 文章の種類ごと

# # %% [markdown]
# # #### 計算

# # %%
# qus_title_terms = qus_terms['表題']
# qus_title_tf, qus_title_tfidf = tfidf(qus_title_terms)

# # %%
# qus_body_terms = qus_terms['質問本文']
# qus_body_tf, qus_body_tfidf = tfidf(qus_body_terms)

# # %%
# ans_body_terms = ans_terms['回答本文']
# ans_body_tf, ans_body_tfidf = tfidf(ans_body_terms)

# # %%
# all_terms = pd.concat([qus_title_terms, qus_body_terms, ans_body_terms])
# all_tf, all_tfidf = tfidf(all_terms)

# # %% [markdown]
# # #### 保存

# # %%
# save.dict(qus_title_tf, '用語', 'TF', 'dst/tf/qus_title_tf.csv')
# save.dict(qus_body_tf, '用語', 'TF', 'dst/tf/qus_body_tf.csv')
# save.dict(ans_body_tf, '用語', 'TF', 'dst/tf/and_body_tf.csv')
# save.dict(all_tf, '用語', 'TF', 'dst/tf/all_tf.csv')

# # %%
# save.dict(qus_title_tfidf, '用語', 'TFIDF', 'dst/tfidf/qus_title_tfidf.csv')
# save.dict(qus_body_tfidf, '用語', 'TFIDF', 'dst/tfidf/qus_body_tfidf.csv')
# save.dict(ans_body_tfidf, '用語', 'TFIDF', 'dst/tfidf/and_body_tfidf.csv')
# save.dict(all_tfidf, '用語', 'TFIDF', 'dst/tfidf/all_tfidf.csv')

if __name__ == '__main__':
    qus_terms, ans_terms = load()

    ans_body_by_question_id = ans_terms.groupby('質問ID').agg({'回答本文': sum}, start=[]).reset_index()
    ans_qus = qus_terms.merge(ans_body_by_question_id)[['カテゴリ名', '表題', '質問本文', '回答本文']]
    terms_by_question_id = ans_qus['表題'] + ans_qus['質問本文'] + ans_qus['回答本文']
    cas_terms_by_question_id = pd.DataFrame({
        'カテゴリ名': ans_qus['カテゴリ名'],
        '用語': terms_by_question_id,
    })

    cas_terms = cas_terms_by_question_id.groupby('カテゴリ名').agg(list)
    cas_tfidf = pd.DataFrame(
        index=cas_terms.index,
        columns={
            'TFIDF': [tfidf(term_lists) for term_lists in cas_terms['用語']]
        }
    )

    for category_name, term_lists in cas_terms.itertuples():
        save.lists('文章ID', pd.Series(term_lists, name='用語'), f"dst/terms/{category_name}.csv")

        tf_dict, tfidf_dict = tfidf(term_lists)
        save.dict(tf_dict, '用語', 'TF', f"dst/tf/{category_name}.csv")
        save.dict(tfidf_dict, '用語', 'TFIDF', f"dst/tfidf/{category_name}.csv")

    qus_title_terms = qus_terms['表題']
    qus_title_tf, qus_title_tfidf = tfidf(qus_title_terms)

    qus_body_terms = qus_terms['質問本文']
    qus_body_tf, qus_body_tfidf = tfidf(qus_body_terms)

    ans_body_terms = ans_terms['回答本文']
    ans_body_tf, ans_body_tfidf = tfidf(ans_body_terms)

    all_terms = pd.concat([qus_title_terms, qus_body_terms, ans_body_terms])
    all_tf, all_tfidf = tfidf(all_terms)

    save.dict(qus_title_tf, '用語', 'TF', 'dst/tf/qus_title_tf.csv')
    save.dict(qus_body_tf, '用語', 'TF', 'dst/tf/qus_body_tf.csv')
    save.dict(ans_body_tf, '用語', 'TF', 'dst/tf/ans_body_tf.csv')
    save.dict(all_tf, '用語', 'TF', 'dst/tf/all_tf.csv')

    save.dict(qus_title_tfidf, '用語', 'TFIDF', 'dst/tfidf/qus_title_tfidf.csv')
    save.dict(qus_body_tfidf, '用語', 'TFIDF', 'dst/tfidf/qus_body_tfidf.csv')
    save.dict(ans_body_tfidf, '用語', 'TFIDF', 'dst/tfidf/ans_body_tfidf.csv')
    save.dict(all_tfidf, '用語', 'TFIDF', 'dst/tfidf/all_tfidf.csv')