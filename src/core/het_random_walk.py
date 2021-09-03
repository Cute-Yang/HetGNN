from collections import deque
import random
from ..util.util import (
    _check_valid_path,
    _split_name_idx
)
from .make_relation import (
    make_br_neigh_list,
    make_rp_neigh_list,
    make_rt_neigh_list,
    make_rw_neigh_list,
    make_rr_neigh_list,
    read_relation_list
)

def make_het_random_walk(
        random_walk_path:str,
        rp_neigh_list,
        br_neigh_list,
        rt_neigh_list,
        rr_neigh_list,
        rw_neigh_list,
        walk_repeat_n:int,
        walk_length:int
    )->None:
    """
    make the het random walk sequence
    Args:
        random_walk_path:str->the file path to save the walk seq
        rp_neigh_list:reporter neigh->(br,rt)
        br_neigh_list:be_reported neigh ->(rp,rt)
        rt_neigh_list:rt->(rp,br,rw,rr)
        rr_neigh_list:rr->(rt)
        rw_neigh_list:rw->(rt)
    """
    _check_valid_path(random_walk_path)
    writer=open(random_walk_path,mode="w",encoding="utf-8")
    size_t=len(rt_neigh_list)
    for _ in range(walk_repeat_n):
        for j in range(size_t):
            rt_neigh=rt_neigh_list[j]
            if len(rt_neigh)==0:
                continue
            walk_seq=deque()
            walk_seq.append("rt_{}".format(j))
            next_node=random.choice(rt_neigh)
            for _ in range(walk_length-1):
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
    writer.close()



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
        random_walk_path="data/het_random_walk/het_random_walk.txt",
        rp_neigh_list=rp_neigh_list,
        br_neigh_list=br_neigh_list,
        rt_neigh_list=rt_neigh_list,
        rr_neigh_list=rr_neigh_list,
        rw_neigh_list=rw_neigh_list,
        walk_repeat_n=10,
        walk_length=50
    )
