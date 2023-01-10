import pandas as pd

import datetime
import re

from multiprocessing import Pool

def subtract(params):
    if pd.isna(params[0]) or pd.isna(params[1]):
        return pd.NA
    return params[0] - params[1]

def divide(params):
    if pd.isna(params[0]) or pd.isna(params[1]):
        return pd.NA
    return params[0] / params[1]

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

def parse_datetime(obj):
    if isinstance(obj, str):
      if obj != '1970-01-01T09:00:00+09:00':
        return datetime.datetime.strptime(obj.replace(':', ''), '%Y-%m-%dT%H%M%S%z')
    return pd.NA

def determine_timezone(dt):
    index = int((dt.hour - 3) % 24 / 8)
    return ('朝', '昼', '夜')[index]

if __name__ == '__main__':
    p = Pool(8)

    def subtract(s1, s2):
        return p.map(subtract, zip(s1, s2), chunksize=1024)

    def divide(s1, s2):
        return p.map(divide, zip(s1, s2), chunksize=1024)

    # データの読み込み
    cas = pd.read_csv('src/categories.csv')
    qus = pd.read_csv('src/questions.csv')
    ans = pd.read_csv('src/answers.csv')

    # 列名の変更
    cas.rename(
        columns = {
            'id': 'カテゴリID',
            'name': 'カテゴリ名',
        },
        inplace=True
    )

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

    # カテゴリIDの変換
    qus = qus.merge(cas).drop('カテゴリID', axis='columns')
    ans = ans.merge(qus[['質問ID', 'カテゴリ名']])

    # テキストの変換
    qus['表題'] = p.map(cleansetext, qus['表題'], chunksize=1024)
    qus['質問本文'] = p.map(cleansetext, qus['質問本文'], chunksize=1024)
    ans['回答本文'] = p.map(cleansetext, ans['回答本文'], chunksize=1024)

    # 日付の変換
    qus['質問日時'] = p.map(parse_datetime, qus['質問日時'], chunksize=1024)

    ans['「ありがとう」日時'] = p.map(parse_datetime, ans['「ありがとう」日時'], chunksize=1024)
    ans['BA日時'] = p.map(parse_datetime, ans['BA日時'], chunksize=1024)
    ans['回答日時'] = p.map(parse_datetime, ans['回答日時'], chunksize=1024)

    # 列の追加
    qus['表題の長さ'] = p.map(len, qus['表題'], chunksize=1024)
    qus['質問本文の長さ'] = p.map(len, qus['質問本文'], chunksize=1024)
    qus['質問本文の長さに対する表題の長さの割合'] = divide(qus['表題の長さ'], qus['質問本文の長さ'])

    qus['質問者回答数'] = subtract(qus['全回答数'], qus['弁護士回答数'])
    qus['弁護士回答数の割合'] = divide(qus['弁護士回答数'], qus['全回答数'])

    qus['質問の時間帯'] = p.map(determine_timezone, qus['質問日時'], chunksize=1024)

    ans_qus_datetime = pd.merge(
        ans[['回答ID', '質問ID', '回答日時']],
        qus[['質問ID', '質問日時']]
    )
    ans_qus_datetime['回答までの時間'] = subtract(ans_qus_datetime['回答日時'], ans_qus_datetime['質問日時'])
    qus = qus.merge(ans_qus_datetime[['質問ID', '回答までの時間']].groupby('質問ID').min().reset_index())

    ans['回答本文の長さ'] = p.map(len, ans['回答本文'], chunksize=1024)

    ans['回答の時間帯'] = p.map(determine_timezone, ans['回答日時'], chunksize=1024)

    ans['「ありがとう」までの時間'] = subtract(ans['「ありがとう」日時'], ans['回答日時'])
    ans['BAまでの時間'] = subtract(ans['BA日時'], ans['回答日時'])

    # インデックスの修正
    qus.set_index('質問ID', inplace=True)
    ans.set_index('回答ID', inplace=True)

    # 保存
    qus.to_csv('src_ex/qus.csv')
    ans.to_csv('src_ex/ans.csv')

    p.close()