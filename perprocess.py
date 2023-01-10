# %% [markdown]
#  必要なライブラリのインポート

# %%
import pandas as pd

import datetime
import re

# %% [markdown]
#  データの読み込み

# %%
cas = pd.read_csv('src/categories.csv')
qus = pd.read_csv('src/questions.csv')
ans = pd.read_csv('src/answers.csv')

# %% [markdown]
#  列名の変更

# %%
cas.rename(
    columns = {
        'id': 'カテゴリID',
        'name': 'カテゴリ名',
    },
    inplace=True
)

# %%
qus.rename(
    columns = {
        'id': '質問ID',
        'userId': '質問者ID',
        'categoryId': 'カテゴリID',
        'title': '表題',
        'body': '質問本文',
        'numOfAnswers': '全回答数',
        'numOfLawyerAnswers': '弁護士回答数',
        'createdAt': '質問日時',
    },
    inplace=True
)

# %%
ans.rename(
    columns = {
        'id': '回答ID',
        'questionId': '質問ID',
        'body': '回答本文',
        'answeredLawyerId': '回答弁護士ID',
        'numOfLawyerAgreement': '同意人数',
        'thankedAt': '「ありがとう」日時',
        'bestAnsweredAt': 'BA日時',
        'createdAt': '回答日時',
    },
    inplace=True
)

# %% [markdown]
#  カテゴリIDの変換

# %%
qus = qus.merge(cas).drop('カテゴリID', axis='columns')
ans = ans.merge(qus[['質問ID', 'カテゴリ名']])

# %% [markdown]
#  テキストの変換

# %%
ZEN = ''.join(chr(0xff01 + i) for i in range(94))
HAN = ''.join(chr(0x21 + i) for i in range(94))
ZEN2HAN = str.maketrans(ZEN, HAN)

NG = ''.join(chr(0x0000 + i) for i in range(33)) + '　'
NGRE = re.compile(f"[{NG}]")

def cleansetext(text):
    result = text
    result = result.translate(ZEN2HAN)
    result = NGRE.sub('', result)
    return result

# %%
qus['表題'] = qus['表題'].apply(cleansetext)
qus['質問本文'] = qus['質問本文'].apply(cleansetext)
ans['回答本文'] = ans['回答本文'].apply(cleansetext)

# %% [markdown]
#  日付の変換

# %%
def parse_datetime(obj):
    if isinstance(obj, str):
      if obj != '1970-01-01T09:00:00+09:00':
        return datetime.datetime.strptime(obj.replace(':', ''), '%Y-%m-%dT%H%M%S%z')
    return pd.NA

# %%
qus['質問日時'] = qus['質問日時'].apply(parse_datetime)

# %%
ans['「ありがとう」日時'] = ans['「ありがとう」日時'].apply(parse_datetime)
ans['BA日時'] = ans['BA日時'].apply(parse_datetime)
ans['回答日時'] = ans['回答日時'].apply(parse_datetime)

# %% [markdown]
#  列の追加

# %%
def determine_timezone(dt):
    index = int((dt.hour - 3) % 24 / 8)
    return ('朝', '昼', '夜')[index]

def subtract(params):
    if pd.isna(params[0]) or pd.isna(params[1]):
        return pd.NA
    return params[0] - params[1]

# %%
qus['表題の長さ'] = qus['表題'].apply(len)
qus['質問本文の長さ'] = qus['質問本文'].apply(len)
qus['質問本文の長さに対する表題の長さの割合'] = qus['表題の長さ'] / qus['質問本文の長さ']

qus['質問者回答数'] = qus['全回答数'] - qus['弁護士回答数']
qus['弁護士回答数の割合'] = qus['弁護士回答数'] / qus['全回答数']

qus['質問の時間帯'] = qus['質問日時'].apply(determine_timezone)

ans_qus_datetime = pd.merge(
    ans[['回答ID', '質問ID', '回答日時']],
    qus[['質問ID', '質問日時']]
)
ans_qus_datetime['回答までの時間'] = [dt1 - dt2 for (dt1, dt2) in zip(ans_qus_datetime['回答日時'], ans_qus_datetime['質問日時'])]
qus = qus.merge(ans_qus_datetime[['質問ID', '回答までの時間']].groupby('質問ID').min().reset_index())

# %%
ans['回答本文の長さ'] = ans['回答本文'].apply(len)

ans['回答の時間帯'] = ans['回答日時'].apply(determine_timezone)

ans['「ありがとう」までの時間'] = [subtract(dts) for dts in zip(ans['「ありがとう」日時'], ans['回答日時'])]
ans['BAまでの時間'] = [subtract(dts) for dts in zip(ans['BA日時'], ans['回答日時'])]

# %% [markdown]
# インデックスの修正

# %%
qus.set_index('質問ID', inplace=True)
ans.set_index('回答ID', inplace=True)

# %% [markdown]
# 保存

# %%
qus.to_csv('src_ex/qus.csv')
ans.to_csv('src_ex/ans.csv')
