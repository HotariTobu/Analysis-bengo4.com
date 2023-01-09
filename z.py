import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

sns.set(font = "IPAexGothic")

qus = pd.read_csv('src_ex/qus.csv')
ans = pd.read_csv('src_ex/ans.csv')

qus['質問日時'] = qus['質問日時'].apply(pd.to_datetime)
qus['回答までの時間'] = qus['回答までの時間'].apply(pd.to_timedelta)

ans['「ありがとう」日時'] = ans['「ありがとう」日時'].apply(pd.to_datetime)
ans['BA日時'] = ans['BA日時'].apply(pd.to_datetime)
ans['回答日時'] = ans['回答日時'].apply(pd.to_datetime)
ans['「ありがとう」までの時間'] = ans['「ありがとう」までの時間'].apply(pd.to_timedelta)
ans['BAまでの時間'] = ans['BAまでの時間'].apply(pd.to_timedelta)

qus_by_cat = qus.groupby('カテゴリ名')
ans_by_cat = ans.groupby('カテゴリ名')

def saveplot(output_path, **kwargs):
    plt.tight_layout()
    plt.savefig(output_path, bbox_inches='tight', **kwargs)


qus_num = qus[['質問ID', '弁護士回答数', '質問者回答数', '表題の長さ', '質問本文の長さ', '質問本文の長さに対する表題の長さの割合', '質問の時間帯']].copy()
qus_num['回答までの時間(秒)'] = [td.total_seconds() for td in qus['回答までの時間']]

ans_num = ans[['質問ID', '同意人数', '回答本文の長さ', '回答の時間帯']].copy()
ans_num['「ありがとう」までの時間(秒)'] = [td.total_seconds() for td in ans['「ありがとう」までの時間']]
ans_num['BAまでの時間(秒)'] = [td.total_seconds() for td in ans['BAまでの時間']]

qus_ans_num = qus_num.merge(ans_num)
qus_ans_num['回答本文の長さに対する表題の長さの割合'] = qus_ans_num['表題の長さ'] / qus_ans_num['回答本文の長さ']
qus_ans_num['回答本文の長さに対する質問本文の長さの割合'] = qus_ans_num['質問本文の長さ'] / qus_ans_num['回答本文の長さ']

g = sns.pairplot(qus_ans_num.drop(['質問ID', '回答の時間帯'], axis='columns'), hue='質問の時間帯')
g._legend.remove()
g.add_legend(loc='upper right')
for ax in g.axes.flatten():
    ax.set_xlabel(ax.get_xlabel(), rotation = 45)
    ax.set_ylabel(ax.get_ylabel(), rotation = 45)
    ax.yaxis.get_label().set_horizontalalignment('right')

saveplot('dst/stats/qus_ans_pair.png', dpi=1000)