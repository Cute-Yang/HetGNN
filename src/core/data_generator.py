"""
generate data 
although we have 5 type node here,but we finall only fetch 4 type node,exclude reporter
"""
from numpy.core.defchararray import center
from numpy.core.fromnumeric import shape
from ..util.auto_log import LogFactory
import os
from .make_relation import (
    make_rr_neigh_list,
    make_rw_neigh_list,
    make_rt_neigh_list,
    make_rp_neigh_list,
    make_br_neigh_list,
    read_relation_list
)
import numpy as np
from ..util.util import _split_name_idx

class DataGenerator(object):
    def __init__(
            self,
            embed_size:int,
            window_size:int,
            relation_file_dict:dict,
            het_random_path:str,
            restart_het_random_path:str,
            walk_L:int,
            batch_size:int,
            random_seed: int=None,
            node_type_number:int=5,
            node_id_len=2,
            **kwargs
        ) -> None:
        """
        Args:
            embed_size:the size of feature len,
            window_size:the window_size
            random_seed:optional param
            relation_file_dict:key->file_path,
            het_random_path:het random walk seq path
            walk_L:int -> the lenth of the walk seq,should be equal ....
            restart_het_random_path:restart het random walk seq path
            node_type_number:the type of the node you have,default is 5
        """
        self.embed_size=embed_size
        self.window_size=window_size
        self.random_seed=random_seed
        self.relation_file_dict=relation_file_dict
        self.walk_L=walk_L
        self.batch_size=batch_size
        self.node_type_number=node_type_number
        self.node_id_len=node_id_len
        #check relation files
        #you should give right file key
        expected_keys=(
            "br_rp", "br_rt", "rp_br", 
            "rp_rt", "rr_rt", "rt_br",
            "rt_rp","rt_rr","rt_rw","rw_rt"
        )
        for expected_key in expected_keys:
            if expected_key not in relation_file_dict:
                raise ValueError("we expected key->{},but not find in {}".format(
                    expected_key,
                    relation_file_dict
                ))
        self.het_random_path=het_random_path
        self.restart_het_random_path=restart_het_random_path
        log_dir=kwargs.get("log_path","logs")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_name=kwargs.get("log_name","data_generator.log")
        self.logger=LogFactory(
            log_dir=log_dir,
            log_prefix=log_name,
            scope_name="data_generator",
            file_handler_type="normal"
        )
        self.node_map = {
            "rp": 0,
            "br": 1,
            "rt": 2,
            "rw": 3,
            "rr": 4
        }
    
    def _fetch_relation_train(self)->None:
        """
        fetch the realtion list from file
        """
        self.logger.info("start to make node relation train list ....")
        br_rp_path=self.relation_file_dict.get("br_rp")
        self.br_rp_train_list=read_relation_list(br_rp_path)

        br_rt_path=self.relation_file_dict.get("br_rt")
        self.br_rt_train_list=read_relation_list(br_rt_path)

        rp_br_path=self.relation_file_dict.get("rp_br")
        self.rp_br_train_list=read_relation_list(rp_br_path)

        rp_rt_path=self.relation_file_dict.get("rp_rt")
        self.rp_rt_train_list=read_relation_list(rp_rt_path)

        rr_rt_path=self.relation_file_dict.get("rr_rt")
        self.rr_rt_train_list=read_relation_list(rr_rt_path)

        rt_br_path=self.relation_file_dict.get("rt_br")
        self.rt_br_train_list=read_relation_list(rt_br_path)
        
        rt_rp_path=self.relation_file_dict.get("rt_rp")
        self.rt_rp_train_list=read_relation_list(rt_rp_path)

        rt_rr_path=self.relation_file_dict.get("rt_rr")
        self.rt_rr_train_list=read_relation_list(rt_rr_path)
        
        rt_rw_path=self.relation_file_dict.get("rt_rw")
        self.rt_rw_train_list=read_relation_list(rt_rw_path)

        rw_rt_path=self.relation_file_dict.get("rw_rt")
        self.rw_rt_train_list=read_relation_list(rw_rt_path)


        #check the valid size of each relation list
        bad_info_fmt="occur size error for {},please check..."
        assert len(self.rp_br_train_list)==len(self.rp_rt_train_list),bad_info_fmt.format("rp size")
        assert len(self.br_rp_train_list)==len(self.br_rt_train_list),bad_info_fmt.format("br size")
        assert len(self.rt_rp_train_list)==len(self.rt_br_train_list)==len(self.rt_rw_train_list)==len(self.rt_rr_train_list),bad_info_fmt.format("rt size")


    def _fetch_neigh_train(self)->None:
        self.logger.info("start to make node neigh list....")
        self.br_neigh_train_list=make_br_neigh_list(
            self.br_rp_train_list,
            self.br_rt_train_list
        )

        self.rp_neigh_train_list=make_rp_neigh_list(
            self.rp_br_train_list,
            self.rp_rt_train_list
        )

        self.rt_neigh_train_list=make_rt_neigh_list(
            self.rt_rp_train_list,
            self.rt_br_train_list,
            self.rt_rr_train_list,
            self.rt_rw_train_list
        )

        self.rw_neigh_train_list=make_rw_neigh_list(
            self.rw_rt_train_list
        )

        self.rr_neigh_train_list=make_rr_neigh_list(
            self.rr_rt_train_list
        )

    
    def compute_sample_prob(self,line_sep=",")->np.ndarray:
        """
        compute the combination prob of the node,just like markov chain a->b(prob)
        Args:
            line_sep:the delimiter of the line
        """
        self.logger.info("start to compute sample prob.....")
        #the combination_number_list just like a symmetric matrix
        combination_number_matrix=np.zeros(shape=(self.node_type_number,self.node_type_number))
        het_random_walk_reader=open(self.het_random_path,mode="r",encoding="utf-8")
        center_node=""
        neigh_node=""

        for idx,line in enumerate(het_random_walk_reader):
            path_list=line.strip().split(line_sep)
            for j in range(self.walk_L):
                center_node=path_list[j]
                node_type=center_node[:self.node_id_len]
                node_type_idx=self.node_map[node_type]
                for k in range(j-self.window_size,j+self.window_size):
                    if k and k<self.walk_L and k!=j:
                        neigh_node=path_list[k]
                        neigh_node_type=neigh_node[:self.node_id_len]
                        neigh_node_type_idx=self.node_map[neigh_node_type]
                        #use this way,you need not to write the loop code
                        combination_number_matrix[node_type_idx,neigh_node_type_idx]+=1
            if (idx+1)%500==0:
                self.logger.info("compute the sample prob,already finished for {} walk seqs".format(idx+1))

        #for numerical stability
        combination_number_matrix=self.batch_size/(combination_number_matrix*10+1e-5)
        return combination_number_matrix

    def find_top_neigh(self,neigh_freq_dict:dict)->None:
        """
        read the random walk seq from o
        find the top_k neigh for each node
        Args:
            neigh_freq_dict:the freq of each node type
            from 
        """
        if len(neigh_freq_dict)!=self.node_type_number:
            raise ValueError("the size of node freq dict: {} not equal to node_type_number".format(
                len(neigh_freq_dict),
                self.node_type_number
            ))
        rp_size=len(self.rp_rt_train_list)
        br_size=len(self.br_rt_train_list)
        rt_size=len(self.rt_rw_train_list)
        rw_size=len(self.rw_rt_train_list)
        rr_size=len(self.rr_rt_train_list)
        
        #here can not use dense array to restore,because the third dimension is sparse,
        #initializ the neigh type s
        rp_neigh_matirx=[[[] for _ in range(rp_size)] for _ in range(self.node_type_number)]
        br_neigh_matrix=[[[] for _ in range(br_size)] for _ in range(self.node_type_number)]
        rt_neigh_matrix=[[[] for _ in range(rt_size)] for _ in range(self.node_type_number)]
        rw_neigh_matrix=[[[] for _ in range(rw_size)] for _ in range(self.node_type_number)]
        rr_neigh_matrix=[[[] for _ in range(rr_size)] for _ in range(self.node_type_number)]

        #use this to reduce if else
        neigh_matrix_map={
            "rp":rp_neigh_matirx,
            "br":br_neigh_matrix,
            "rt":rt_neigh_matrix,
            "rw":rw_neigh_matrix,
            "rr":rr_neigh_matrix
        }

        #read restart random walk seq
        restart_het_random_reader=open(self.restart_het_random_path,"r",encoding="utf-8")
        for line in restart_het_random_reader:
            line=line.strip()
            first_split=line.split(":")
            center_node,neigh_nodes=first_split[0],first_split[1]
            neigh_node_seq=neigh_nodes.split(",")
            node_name,node_id=_split_name_idx(center_node)
            size_neigh_seq=len(neigh_node_seq)

            #fetch the matrix we need to insert
            neigh_matrix=neigh_matrix_map[node_name]

            for i in range(size_neigh_seq):
                next_node=neigh_node_seq[i]
                next_node_idx,next_node_name=_split_name_idx(next_node)
                #fetch the next node type
                next_node_type=self.node_map[next_node_name]
                neigh_matrix[next_node_type][node_id].append(next_node_idx)
        #close IO
        restart_het_random_reader.close()
            
        #store the neighbor set (based on freq) from random het walk
        
                
                
        
        
    
    

            
        
        
        

    
        

    


    
    

    
        
        

    
