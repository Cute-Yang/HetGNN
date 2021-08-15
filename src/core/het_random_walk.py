import argparse
from collections import deque
import random
arg_parser=argparse.ArgumentParser("het random walk")


def _split_name_idx(node:str):
    name,idx=node.split("_",1)
    try:
        idx=int(idx)
    except:
        idx=-1
    return name,idx

def _split_neighs(row_list,name:str,delimiter=","):
    row_items=["{}_{}".format(name,idx) for idx in row_list]
    return row_items

def make_rp_neigh_list(rp_br_list:list,rp_rt_list:list,delimiter=","):
    assert len(rp_br_list)==len(rp_rt_list),"data size_t error!"
    size_t=len(rp_br_list)
    rp_neigh_list=[[] for _ in range(size_t)]
    for i in range(size_t):
        n_br=rp_br_list[i]
        n_br_nodes=_split_neighs(n_br,name="br",delimiter=delimiter)
        n_rt=rp_rt_list[i]
        n_rt_nodes=_split_neighs(n_rt,name="rt",delimiter=delimiter)
        for item in (n_br_nodes,n_rt_nodes):
            rp_neigh_list[i].extend(item)
    return rp_neigh_list
        

def make_br_neigh_list(br_rp_list:list,br_rt_list,delimiter=","):
    assert len(br_rp_list)==len(br_rt_list),"data size_t error!"
    size_t=len(br_rp_list)
    br_neigh_list=[[] for _ in range(size_t)]
    for i in range(size_t):
        n_rp=br_rp_list[i]
        n_rp_nodes=_split_neighs(n_rp,name="rp",delimiter=delimiter)
        n_rt=br_rt_list[i]
        n_rt_nodes=_split_neighs(n_rt,name="rt",delimiter=delimiter)
        for item in (n_rp_nodes,n_rt_nodes):
            br_neigh_list[i].extend(item)
    return br_neigh_list

def make_rt_neigh_list(rt_rp_list:list,rt_br_list:list,rt_rr_list:list,rt_rw_list:list,delimiter=","):
    size_rp=len(rt_rp_list)
    size_br=len(rt_br_list)
    size_rr=len(rt_rr_list)
    size_rw=len(rt_rw_list)
    assert size_rp==size_br==size_rr==size_rw,"data size_t error!"
    n=size_rp
    rt_neigh_list=[[] for _ in range(n)]
    for i in range(n):
        n_rp=rt_rw_list[i]
        n_rp_nodes=_split_neighs(n_rp,name="rp",delimiter=delimiter)
        n_br=rt_br_list[i]
        n_br_nodes=_split_neighs(n_br,name="br",delimiter=delimiter)
        n_rr=rt_rr_list[i]
        n_rr_nodes=_split_neighs(n_rr,name="rr",delimiter=delimiter)
        n_rw=rt_rw_list[i]
        n_rw_nodes=_split_neighs(n_rw,name="rw",delimiter=delimiter)
        for item in (n_rp_nodes,n_br_nodes,n_rr_nodes,n_rw_nodes):
            rt_neigh_list[i].extend(item)
    return rt_neigh_list

def make_rr_neigh_list(rr_rt_list:list,delimiter=","):
    size_r=len(rr_rt_list)
    rr_neigh_list=[[] for _ in range(size_r)]
    for i in range(size_r):
        n_rt=rr_rt_list[i]
        n_rt_nodes=_split_neighs(n_rt,name="rt",delimiter=delimiter)
        rr_neigh_list[i].extend(n_rt_nodes)
    return rr_neigh_list

def make_rw_neigh_list(rw_rt_list:list,delimiter=","):
    size_w=len(rw_rt_list)
    rw_neigh_list=[[] for _ in range(size_w)]
    for i in range(size_w):
        n_rt=rw_rt_list[i]
        n_rt_nodes=_split_neighs(n_rt,name="rt",delimiter=delimiter)
        rw_neigh_list[i].extend(n_rt_nodes)
    return rw_neigh_list

def make_het_random_walk(
        random_walk_fp:str,
        rp_neigh_list,
        br_neigh_list,
        rt_neigh_list,
        rr_neigh_list,
        rw_neigh_list,
        walk_repeat_n:int,
        walk_length:int
    ):
    
    writer=open(random_walk_fp,mode="w",encoding="utf-8")
    size_t=len(rt_neigh_list)
    for i in range(walk_repeat_n):
        for j in range(size_t):
            rt_neigh=rt_neigh_list[j]
            if len(rt_neigh)==0:
                continue
            walk_seq=deque()
            walk_seq.append("rt_{}".format(j+1))
            next_node=random.choice(rt_neigh)
            for k in range(walk_length-1):
                name,idx=_split_name_idx(next_node)
                if idx==-1:
                    continue
                walk_seq.append(next_node)
                idx=idx-1
                if name=="rp":
                    next_node=random.choice(rp_neigh_list[idx])
                elif name=="br":
                    next_node=random.choice(br_neigh_list[idx])
                elif name=="rr":
                    next_node=random.choice(rr_neigh_list[idx])
                elif name=="rw":
                    next_node=random.choice(rw_neigh_list[idx])
                elif name=="rt":
                    next_node=random.choice(rt_neigh_list[idx])
                else:
                    raise ValueError("unknown node type:{}".format(name))
            if len(walk_seq)<30:
                continue
            seq_line=" ".join(walk_seq)
            writer.write(seq_line)
            writer.write("\n")


def read_relation_list(relation_fp:str,delimiter=","):
    with open(relation_fp,"r",encoding="utf-8") as f:
        relation_list=[]
        for line in f:
            line_split=line.strip().split(":",1)
            idx,neigh=line_split
            neigh_split=neigh.split(delimiter)
            relation_list.append(neigh_split)
    return relation_list

if __name__=="__main__":
    br_rp_list=read_relation_list("data/graph_data/br_rp_train.txt")
    br_rt_list=read_relation_list("data/graph_data/br_rt_train.txt")
    rp_br_list=read_relation_list("data/graph_data/rp_br_train.txt")
    rp_rt_list=read_relation_list("data/graph_data/rp_rt_train.txt")
    rr_rt_list=read_relation_list("data/graph_data/rr_rt_train.txt")
    rt_br_list=read_relation_list("data/graph_data/rt_br_train.txt")
    rt_rp_list=read_relation_list("data/graph_data/rt_rp_train.txt")
    rt_rr_list=read_relation_list("data/graph_data/rt_rr_train.txt")
    rt_rw_list=read_relation_list("data/graph_data/rt_rw_train.txt")
    rw_rt_list=read_relation_list("data/graph_data/rw_rt_train.txt")

    br_neigh_list=make_br_neigh_list(br_rp_list,br_rp_list)
    rp_neigh_list=make_rp_neigh_list(rp_br_list,rp_rt_list)
    rr_neigh_list=make_rr_neigh_list(rr_rt_list)
    rt_neigh_list=make_rt_neigh_list(rt_rp_list,rt_br_list,rt_rr_list,rt_rw_list)
    rw_neigh_list=make_rw_neigh_list(rw_rt_list)

    make_het_random_walk(
        random_walk_fp="data/het_random_walk/het_random_walk.txt",
        rp_neigh_list=rp_neigh_list,
        br_neigh_list=br_neigh_list,
        rt_neigh_list=rt_neigh_list,
        rr_neigh_list=rr_neigh_list,
        rw_neigh_list=rw_neigh_list,
        walk_repeat_n=10,
        walk_length=50
    )
    
        