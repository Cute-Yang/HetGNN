"""
use node 2 vec,get a node net embedding!
read the random walk sequence and use word2vec alg to train
"""

from gensim.models import Word2Vec
from ..util.util import _check_valid_path

def read_random_walk_list(random_walk_fp:str,line_sep=",")->list:
    """
    read from random walk file path and return the sequence list

    Args:
        random_walk_fp:the random walk file path->str
        line_sep:the line delimiter -> str,default is ','
    """
    random_walk_list=[]
    with open(random_walk_fp,mode="r",encoding="utf-8") as f:
        for line in f:
            line_split=line.strip().split(line_sep)
            random_walk_list.append(line_split)
    return random_walk_list


def make_node2vec(random_walk_seq:list,embedding_save_path:str,**kwargs)->None:
    """
    train a node embedding vecotr from random walk seq,with word2vec algrithm
    Args:
        random_walk_seq:essential->list,you random walk sequence
        embbeding_save_path:essential->str,your embbeding vecotr save path
        kwargs:optional,mostly the word2vec train param,if not specify,will use default value
    Returns:
        None
    """
    _check_valid_path(embedding_save_path)
    #read the train param from kwargs
    sg=kwargs.get("sg",1)
    vector_size=kwargs.get("size",200)
    window=kwargs.get("window",5)
    min_count=kwargs.get("min_count",5)
    negative=kwargs.get("negative",3)
    sample=kwargs.get("sample",1e-3)
    hs=kwargs.get("hs",1)
    #the concurrent when training
    workers=kwargs.get("workers",10)
    epochs=kwargs.get("epochs",10)
    
    model=Word2Vec(
        random_walk_seq,
        vector_size=vector_size,
        sg=sg,
        min_count=min_count,
        sample=sample,
        negative=negative,
        hs=hs,
        window=window,
        workers=workers,
        epochs=epochs
    )
    print("Output.....")
    model.wv.save_word2vec_format(embedding_save_path)

