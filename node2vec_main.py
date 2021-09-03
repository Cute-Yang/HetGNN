from src.core import node2vec

if __name__=="__main__":
    embedding_file="data/embed_data/node_net_embedding.txt"
    random_walk_file="data/het_random_walk/het_random_walk.txt"
    het_random_walk_list=node2vec.read_random_walk_list(random_walk_fp=random_walk_file,line_sep=" ")
    node2vec.make_node2vec(het_random_walk_list,embedding_save_path=embedding_file)
    