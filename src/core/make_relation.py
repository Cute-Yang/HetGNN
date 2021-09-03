import random

def _split_name_idx(node: str):
    name, idx = node.split("_", 1)
    try:
        idx = int(idx)
    except:
        idx = -1
    return name, idx


def _concat_node_name(row_list, name: str):
    row_items = ["{}_{}".format(name, idx) for idx in row_list]
    return row_items


def make_rp_neigh_list(rp_br_list: list, rp_rt_list: list) -> list:
    size_t = len(rp_br_list)
    rp_neigh_list = [[] for _ in range(size_t)]
    for i in range(size_t):
        n_br = rp_br_list[i]
        n_br_nodes = _concat_node_name(n_br, name="br")
        n_rt = rp_rt_list[i]
        n_rt_nodes = _concat_node_name(n_rt, name="rt")
        for item in (n_br_nodes, n_rt_nodes):
            rp_neigh_list[i].extend(item)
        random.shuffle(rp_neigh_list[i])
    return rp_neigh_list


def make_br_neigh_list(br_rp_list: list, br_rt_list: list):
    size_t = len(br_rp_list)
    br_neigh_list = [[] for _ in range(size_t)]
    for i in range(size_t):
        n_rp = br_rp_list[i]
        n_rp_nodes = _concat_node_name(n_rp, name="rp")
        n_rt = br_rt_list[i]
        n_rt_nodes = _concat_node_name(n_rt, name="rt")
        for item in (n_rp_nodes, n_rt_nodes):
            br_neigh_list[i].extend(item)
        random.shuffle(br_neigh_list[i])
    return br_neigh_list


def make_rt_neigh_list(rt_rp_list, rt_br_list: list, rt_rr_list: list, rt_rw_list: list):
    size_rp = len(rt_rp_list)
    size_br = len(rt_br_list)
    size_rr = len(rt_rr_list)
    size_rw = len(rt_rw_list)
    assert size_br == size_rr == size_rw, "data size_t error!"
    n = size_br
    rt_neigh_list = [[] for _ in range(n)]
    for i in range(n):
        n_rp = rt_rp_list[i]
        n_rp_nodes = _concat_node_name(n_rp, name="rp")
        n_br = rt_br_list[i]
        n_br_nodes = _concat_node_name(n_br, name="br")
        n_rr = rt_rr_list[i]
        n_rr_nodes = _concat_node_name(n_rr, name="rr")
        n_rw = rt_rw_list[i]
        n_rw_nodes = _concat_node_name(n_rw, name="rw")
        for item in (n_rp_nodes, n_br_nodes, n_rr_nodes, n_rw_nodes):
            rt_neigh_list[i].extend(item)
        random.shuffle(rt_neigh_list[i])
    return rt_neigh_list


def make_rr_neigh_list(rr_rt_list: list):
    size_r = len(rr_rt_list)
    rr_neigh_list = [[] for _ in range(size_r)]
    for i in range(size_r):
        n_rt = rr_rt_list[i]
        n_rt_nodes = _concat_node_name(n_rt, name="rt")
        rr_neigh_list[i].extend(n_rt_nodes)
        random.shuffle(rr_neigh_list[i])
    return rr_neigh_list


def make_rw_neigh_list(rw_rt_list: list):
    size_w = len(rw_rt_list)
    rw_neigh_list = [[] for _ in range(size_w)]
    for i in range(size_w):
        n_rt = rw_rt_list[i]
        n_rt_nodes = _concat_node_name(n_rt, name="rt")
        rw_neigh_list[i].extend(n_rt_nodes)
        random.shuffle(rw_neigh_list[i])
    return rw_neigh_list


def read_relation_list(relation_fp: str, delimiter=","):
    with open(relation_fp, "r", encoding="utf-8") as f:
        relation_list = []
        for line in f:
            line_split = line.strip().split(":", 1)
            _, neigh = line_split
            neigh_split = neigh.split(delimiter)
            relation_list.append(neigh_split)
    return relation_list
