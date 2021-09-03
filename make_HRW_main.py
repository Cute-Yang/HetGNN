from src.core.het_random_walk import (
    make_het_random_walk,read_relation_list,
    make_br_neigh_list,
    make_rp_neigh_list,
    make_rr_neigh_list,
    make_rt_neigh_list,
    make_rw_neigh_list
)

if __name__=="__main__":
    br_rp_list = read_relation_list("data/graph_data/br_rp_train.txt")
    br_rt_list = read_relation_list("data/graph_data/br_rt_train.txt")
    rp_br_list = read_relation_list("data/graph_data/rp_br_train.txt")
    rp_rt_list = read_relation_list("data/graph_data/rp_rt_train.txt")
    rr_rt_list = read_relation_list("data/graph_data/rr_rt_train.txt")
    rt_br_list = read_relation_list("data/graph_data/rt_br_train.txt")
    rt_rp_list = read_relation_list("data/graph_data/rt_rp_train.txt")
    rt_rr_list = read_relation_list("data/graph_data/rt_rr_train.txt")
    rt_rw_list = read_relation_list("data/graph_data/rt_rw_train.txt")
    rw_rt_list = read_relation_list("data/graph_data/rw_rt_train.txt")

    br_neigh_list = make_br_neigh_list(br_rp_list, br_rp_list)
    rp_neigh_list = make_rp_neigh_list(rp_br_list, rp_rt_list)
    rr_neigh_list = make_rr_neigh_list(rr_rt_list)
    rt_neigh_list = make_rt_neigh_list(
        rt_rp_list, rt_br_list, rt_rr_list, rt_rw_list)
    rw_neigh_list = make_rw_neigh_list(rw_rt_list)

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
