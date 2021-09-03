from src.core.het_random_walk import read_relation_list
from src.core.restart_random_walk import Restart_HRW

if __name__=="__main__":
    br_rp_train_fp="data/graph_data/br_rp_train.txt"
    br_rp_train_list=read_relation_list(br_rp_train_fp)
    br_rt_train_fp="data/graph_data/br_rt_train.txt"
    br_rt_train_list=read_relation_list(br_rt_train_fp)

    rp_br_train_fp="data/graph_data/rp_br_train.txt"
    rp_br_train_list=read_relation_list(rp_br_train_fp)
    rp_rt_train_fp="data/graph_data/rp_rt_train.txt"
    rp_rt_train_list=read_relation_list(rp_rt_train_fp)

    rr_rt_train_fp="data/graph_data/rr_rt_train.txt"
    rr_rt_train_list=read_relation_list(rr_rt_train_fp)

    rt_br_train_fp="data/graph_data/rt_br_train.txt"
    rt_br_train_list=read_relation_list(rt_br_train_fp)
    rt_rp_train_fp="data/graph_data/rt_rp_train.txt"
    rt_rp_train_list=read_relation_list(rt_rp_train_fp)
    rt_rr_train_fp="data/graph_data/rt_rr_train.txt"
    rt_rr_train_list=read_relation_list(rt_rr_train_fp)
    rt_rw_train_fp="data/graph_data/rt_rw_train.txt"
    rt_rw_train_list=read_relation_list(rt_rw_train_fp)

    rr_rt_train_fp="data/graph_data/rr_rt_train.txt"
    rr_rt_train_list=read_relation_list(rr_rt_train_fp)

    rw_rt_train_fp="data/graph_data/rw_rt_train.txt"
    rw_rt_train_list=read_relation_list(rw_rt_train_fp)

    restart_walker=Restart_HRW(
        br_rp_train_list=br_rp_train_list,
        br_rt_train_list=br_rt_train_list,
        rp_br_train_list=rp_br_train_list,
        rp_rt_train_list=rp_rt_train_list,
        rt_br_train_list=rt_br_train_list,
        rt_rp_train_list=rt_rp_train_list,
        rt_rr_train_list=rt_rr_train_list,
        rt_rw_train_list=rt_rw_train_list,
        rr_rt_train_list=rr_rt_train_list,
        rw_rt_train_list=rw_rt_train_list,
        prob_thresh=0.5
    )
    
    restart_walker.restart_het_random_walk(
        rp_sample_number=30,
        br_sample_number=30,
        rt_sample_number=30,
        rr_sampele_number=5,
        rw_sample_number=30
    )
