import argparse
import json
import os
import re

import jieba
import networkx as nx
import numpy as np
import pandas as pd
from nltk import ngrams
from sklearn.feature_extraction.text import TfidfVectorizer


def generate_id(key, key_dict)->int:
    if key in key_dict:
        index = len(key_dict)
        key_dict[key] = index
    else:
        index = key_dict[key]
    return index


def generate_words(words_file, data_path):
    lines = open(words_file, encoding="utf-8").readlines()
    header = lines[0]
    header_2_index = dict(zip(header, range(len(header))))

    key_words_2_id = {}
    key_words_2_report_ids = {}

    report_id_2_id = {}
    report_id_2_key_words = {}
    report_id_2_uin = {}

    uin_2_id = {}
    uin_2_report = {}
    evil_uin_2_report = {}

    G = nx.Graph()
    for i in range(1, len(lines)):
        line = lines[i]
        fields = line.split('\001')
        str_uin = fields[header_2_index["strUin"]]
        str_evil_uin = fields[header_2_index["strEvilUin"]]
        text = fields[header_2_index["sentence"]]
        report_id = fields[header_2_index["id"]]
        words = fields[header_2_index["words"]]
        key_words = fields[header_2_index["key_words"]].split('|')
        key_words = [key_words.split("-")[0] for x in key_words]

        str_uin_index = generate_id(str_uin, uin_2_id)
        str_evil_uin_index = generate_id(str_evil_uin, uin_2_id)
        report_index = generate_id(report_id, report_id_2_id)

        # 举报邻居节点
        report_id_2_uin[report_index] = "{},{}".format(
            str_uin_index, str_evil_uin_index)
        if str_uin_index not in uin_2_report:
            uin_2_report[str_uin_index] = []
        uin_2_report[str_uin_index].append(report_index)

        # 被举报邻居节点
        if str_evil_uin_index not in evil_uin_2_report:
            evil_uin_2_report[str_evil_uin_index] = []
        evil_uin_2_report[str_evil_uin_index].append(report_index)

        # 关键词
        if report_index not in report_id_2_key_words:
            report_id_2_key_words[report_index] = []
        for kw in key_words:
            kw_index = generate_id(kw, key_words_2_id)
            if kw_index not in key_words_2_report_ids:
                key_words_2_report_ids[kw_index] = []
                report_id_2_key_words[report_index].append(key_words)
            report_id_2_key_words[kw_index].append(report_index)

    # 关键词
    json.dump(key_words_2_id, open(os.path.join(
        data_path, "key_words_map.csv"), 'w'), indent=2)
    # 用户
    json.dump(uin_2_id, open(os.path.join(
        data_path, "uin_map.csv"), 'w'), indent=2)
    # 举报
    json.dump(report_id_2_id, open(os.path.join(
        data_path, "report_map.csv"), 'w'), indent=2)


def tfidf_weight(df):
    lines = open(words_file, encoding="utf-8").readlines()
    words_list = []
    words_lenth = []
    header = lines[0]
    for i in range(1, len(lines)):
        line = lines[i].strip()
        words = line.split('\001')[-1].split(' ')
        words_lenth.append(len(words))
        words_list.append(' '.join(words))
    print("words_lenth:{}".format(words_lenth[:20]))
    print("平均字数长度{}".format(np.mean(words_lenth)))
    tfidfdict = {}
    # vectorizer = TfidfVectorizer(token_pattern=r"(?u)\b\w+\b", ngram_range=(1, 2), max_df=0.8,
    #                              min_df=10)  # 该类会统计每个词语的tf-idf权值
    vectorizer = TfidfVectorizer(max_df=0.5, min_df=10)  # 该类会统计每个词语的tf-idf权值
    # 第一个fit_transform是计算tf-idf，第二个fit_transform是将文本转为词频矩阵
    tfidf = vectorizer.fit_transform(words_list)
    tfidf_word = vectorizer.get_feature_names()  # 获取词袋模型中的所有词语
    print("词表大小{}".format(len(tfidf_word)))
    index_2_tfidf_word = dict(zip(range(len(tfidf_word)), tfidf_word))
    weight = tfidf.toarray()  # 将tf-idf矩阵抽取出来，元素a[i][j]表示j词在i类文本中的tf-idf权重
    top_words_list = []
    for i in range(len(weight)):  # 打印每类文本的tf-idf词语权重，第一个for遍历所有文本，第二个for便利某一类文本下的词语权重
        sort = np.argsort(weight[i])
        top = sort[-20:][::-1]
        top_words = ["{}-{:.3f}".format(index_2_tfidf_word[idx], weight[i][idx])
                     for idx in top if weight[i][idx] > 0]
        top_words_list.append(top_words)

    out = '\n'.join([lines[i + 1].strip() + '\001' +
                    '|'.join(top_words_list[i]) for i in range(len(lines) - 1)])
    writer = open(tfidf_words_file, "w")
    writer.write("{}\001{}\n".format(header.strip(), "key_words"))
    writer.write(out)
    writer.close()


def get_ngram(candicate_list, n=2):
    new_words = []
    if len(candicate_list) == 0:
        return new_words
    for gs in ngrams(candicate_list, n):
        # 叠词无意义，剔除
        word = ""
        for g in gs:
            if word == g:
                continue
            word += g
        if word != "":
            new_words.append(word)
    return new_words


def simplify_report(df, words_file):
    def combine_report(data):
        sentence_set = set()
        id_set = set()
        for i in range(len(data)):
            ctx = data.iloc[i]["strImpeachSrvParam"]
            id = data.iloc[i]["id"]
            ctx_fields = ctx.split("|")
            for c in ctx_fields:
                if not c.startswith("被举报人") and not c.startswith("其他人") and not c.startswith("群主其他人"):
                    continue
                values = c.split(":")
                words = ':'.join(values[1:])
                words = words.replace('\001', ' ')
                words = words.replace('\002', ' ')
                words = words.replace('|', ' ')
                sentence_set.add(words)
            id_set.add(id)

        new_words = []
        # 一句话一句话分词
        for sen in sentence_set:
            sen = sen.upper()
            sen = re.sub('\[[^[]+?\]', '', sen)
            pattern = re.compile(r'[^a-zA-Z\u4e00-\u9fa5]')
            sen = re.sub(pattern, '', sen)
            # 只有一个词，剔除
            if len(sen) == 1:
                continue
            # 有两个词单做一个单词
            elif len(sen) == 2:
                new_words.append(sen)
                continue
            # 3个词以上，jieba分词，同时取bigram
            else:
                for w in jieba.lcut(sen, cut_all=True):
                    w = w.strip()
                    # 停用词、空串剔除
                    if w == "":
                        continue
                    if w in stopwords:
                        continue
                    new_words.append(w)
                #     # 按顺序拼接
                #     candicate_list.append(w)
                # bigram = get_ngram(candicate_list, 2)
                # new_words.extend(bigram)

        if len(new_words) < 5:
            return pd.Series({
                "sentence": '|'.join(sentence_set),
                "id": ','.join(id_set),
                "words": ""
            })
        return pd.Series({
            "sentence": '|'.join(sentence_set),
            "id": ','.join(id_set),
            "words": ' '.join(new_words)
        })

    combine_df = df.groupby(by=["strUin", "strEvilUin"]).apply(combine_report)
    print("根据举报人被举报人去重的投诉数据量{}".format(len(combine_df)))
    combine_df = combine_df.reset_index()
    # 根据举报人、被举报人去重，抽取其他人和被举报人说的话，剔除空的内容后剩余9k条数据
    # 97381/126009 = 77%
    combine_df = combine_df[combine_df["words"] != ""].copy()
    print("根据举报人被举报人去重，剔除无效文本的投诉数据量{}".format(len(combine_df)))
    print(combine_df.head())
    combine_df.to_csv(words_file, sep='\001', index=False)
    return combine_df


def draw_words(org_input):
    reader = open(org_input, mode="r", encoding="utf-8")
    header = reader.readline().strip().split('\001')
    header_2_index = dict(zip(header, range(len(header))))
    print("header_2_index", header_2_index)
    data = []
    while True:
        line = reader.readline().strip()
        if not line:
            break
        line_split = line.strip().split("\001")
        if len(line_split) != 23:
            continue
        chat_ctx = line_split[header_2_index["strImpeachSrvParam"]]
        if chat_ctx == "":
            continue
        data.append(line_split)
    if len(data) == 0:
        return None
    print("有投诉内容的投诉数据量{}".format(len(data)))
    print("列名{}".format(header))
    df = pd.DataFrame(data)
    df.columns = header
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='tfidf key words')
    # parser.add_argument('--data_path', type=str, default='/Users/xiangyuwang/Works/KFBusiness/tx_defender/',
    #                     help='path to data')
    parser.add_argument('--data_path', type=str, default='/dockerdata/gisellewang/sif_cluster/',
                        help='path to data')
    parser.add_argument('--top_k_words', type=int, default=10,
                        help='top k tfidf key words')

    args = parser.parse_args()
    print(args)

    data_path = os.path.join(
        args.data_path, "data/kf_report_scene_2021-06-28.txt")
    words_file = os.path.join(args.data_path, "data/words_20210628.csv")
    words_emd_file = os.path.join(args.data_path, "data/words_emds.csv")

    tfidf_words_file = os.path.join(
        args.data_path, "data/tfidf_words_20210628.csv")
    w2v_fp = os.path.join(
        args.data_path, "resource/Tencent_AILab_ChineseEmbedding.txt")
    userdict_fp = os.path.join(args.data_path, "resource/userdict.txt")
    stopwords_fp = os.path.join(args.data_path, "resource/stopword.txt")

    # 读取停用词
    with open(stopwords_fp, "r", encoding="utf-8") as f:
        stopwords = set([word.strip() for word in f])
    # jieba 加载用户自定义词
    userdict_lines = open(userdict_fp).readlines()
    for w in userdict_lines:
        jieba.add_word(w.strip())
    print("data_path", data_path)
    df = draw_words(data_path)
    simplify_df = simplify_report(df, words_file)
