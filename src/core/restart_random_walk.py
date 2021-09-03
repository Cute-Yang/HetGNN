"""
restart random walk to sample neigh node
"""
from posixpath import dirname
from numpy.core.fromnumeric import size
from ..util.util import _split_name_idx
import random
import os

#the prob that we can back to the start 
PROB_THRESH=0.5

from .make_relation import (
    make_rp_neigh_list,
    make_br_neigh_list,
    make_rt_neigh_list,
    make_rw_neigh_list,
    make_rr_neigh_list
)


def _size_check(*args)->bool:
    """
    check each of the args's size 
    """
    flag=True
    size_arg=len(args)
    for i in range(size_arg-1):
        flag=(len(args[i])==len(args[i+1]))
        if not flag:
            return flag
    return flag


class Restart_HRW:
    def __init__(
            self,
            br_rp_train_list,
            br_rt_train_list,
            rp_br_train_list,
            rp_rt_train_list,
            rt_br_train_list,
            rt_rp_train_list,
            rt_rr_train_list,
            rt_rw_train_list,
            rr_rt_train_list,
            rw_rt_train_list,
            prob_thresh:float=0.5
        ):
        error_info_fmt = "the size beween {} is not equal!"

        _br_check_status=_size_check(br_rp_train_list,br_rt_train_list)
        if not _br_check_status:
            raise ValueError(error_info_fmt.format(("br_rp","br_rt")))
            
        _rp_check_status=_size_check(rp_rt_train_list,rp_br_train_list)
        if not _rp_check_status:
            raise ValueError(error_info_fmt.format(("rp_rt","rp_br")))
    
        _rt_check_status=_size_check(rt_rp_train_list,rt_br_train_list,rt_rw_train_list,rt_rr_train_list)
        if not _rt_check_status:
            size_info="rt_rp:{}\nrt_br:{}\nrt_rw:{}\nrt_rr{}".format(
                len(rt_rp_train_list),
                len(rt_br_train_list),
                len(rt_rw_train_list),
                len(rt_rr_train_list)
            )
            print(size_info)
            raise ValueError(error_info_fmt.format(("rt_rp","rt_br","rt_rw","rt_rr")))
        
        self.br_rp_train_list=br_rp_train_list
        self.br_rt_train_list=br_rt_train_list
        self.rp_br_train_list=rp_br_train_list
        self.rp_rt_train_list=rp_rt_train_list
        self.rt_br_train_list=rt_br_train_list
        self.rt_rp_train_list=rt_rp_train_list
        self.rt_rr_train_list=rt_rr_train_list
        self.rt_rw_train_list=rt_rw_train_list
        self.rr_rt_train_list=rr_rt_train_list
        self.rw_rt_train_list=rw_rt_train_list
        #restart prob
        self.prob_thresh=prob_thresh

        #make neighboor node train list
        self.br_neigh_train_list=make_br_neigh_list(self.br_rp_train_list,self.br_rt_train_list)
        self.rp_neigh_train_list=make_rp_neigh_list(self.rp_br_train_list,self.rp_rt_train_list)
        self.rt_neigh_train_list=make_rt_neigh_list(self.rt_rp_train_list,self.rt_br_train_list,self.rt_rr_train_list,self.rt_rw_train_list)
        self.rr_neigh_train_list=make_rr_neigh_list(self.rr_rt_train_list)
        self.rw_neigh_train_list=make_rw_neigh_list(self.rw_rt_train_list)
        

    def restart_het_random_walk(
            self,
            rp_sample_number:int,
            br_sample_number:int,
            rt_sample_number:int,
            rr_sampele_number:int,
            rw_sample_number:int,
            restart_het_random_walk_fp:str="data/restart_het_random_walk/restart_het_random_walk.txt",
        )->None:
        """
        Args:
            relation_list:
            br_rp_train_list:br->rp 
            br_rt_train_list:br->rt
            ......
            rw_rt_train_list:rw->rt 
            restart_HRW_save_path:restart random walk sequence file path

            sample_neigh_number:
            rp_sample_number:int->the number of reporter sample
            br_sample_number:int->the number of be_reported sample
            rt_sample_number:int->the number of report text sample
            rr_neigh_number:int -> the number of report reason sample
            rw_neigh_number:int -> the number of report keywords sample

            het_random_walk_fp:str->het random walk file path
        Returns:
            None
        """
        dst_dirname=os.path.dirname(restart_het_random_walk_fp)
        if not os.path.exists(dst_dirname):
            print("make dir {} to save walk seqs...".format(dst_dirname))
            os.makedirs(dst_dirname)

        #the total number to neigh sample
        total_sample_number=rp_sample_number+br_sample_number+rt_sample_number+rr_sampele_number+rw_sample_number

        size_rp=len(self.rp_br_train_list)
        size_br=len(self.br_rt_train_list)
        size_rt=len(self.rt_rw_train_list)
        size_rr=len(self.rr_rt_train_list)
        size_rw=len(self.rw_rt_train_list)
        node_n_list=[size_rp,size_br,size_rt,size_rr,size_rw]
    
        
        rp_neigh_random_walk=[[] for _ in range(size_rp)]
        br_neigh_random_walk=[[] for _ in range(size_br)]
        rt_neigh_random_walk=[[] for _ in range(size_rt)]
        rr_neigh_random_walk=[[] for _ in range(size_rr)]
        rw_neigh_random_walk=[[] for _ in range(size_rw)]
        
        size_node=len(node_n_list)
        for i in range(size_node):
            node_n=node_n_list[i]
            for j in range(node_n):
                if i==0:
                    neigh_temp=self.rp_br_train_list[j]
                    neigh_train=rp_neigh_random_walk[j]
                    curNode="rp_{}".format(j)
                elif i==1:
                    neigh_temp=self.br_rp_train_list[j]
                    neigh_train=br_neigh_random_walk[j]
                    curNode="br_{}".format(j)
                elif i==2:
                    neigh_temp=self.rt_br_train_list[j]
                    neigh_train=rt_neigh_random_walk[j]
                    curNode="rt_{}".format(j)
                elif i==3:
                    neigh_temp=self.rr_rt_train_list[j]
                    neigh_train=rr_neigh_random_walk[j]
                    curNode="rr_{}".format(j)
                elif i==4:
                    neigh_temp=self.rw_rt_train_list[j]
                    neigh_train=rw_neigh_random_walk[j]
                    curNode="rw_{}".format(j)
                if len(neigh_temp)>0:
                    neigh_L=0
                    rp_L=0
                    br_L=0
                    rt_L=0
                    rw_L=0
                    rr_L=0
                    while neigh_L<total_sample_number:
                        start_prob=random.random()
                        curNode_name,curNode_idx=_split_name_idx(curNode)
                        if start_prob>self.prob_thresh:

                            #to avoid mutual references nodes
                            #choose from reporter
                            if curNode_name=="rp":
                                curNode=random.choice(self.rp_neigh_train_list[curNode_idx])
                                curNode_name,_=_split_name_idx(curNode)
                                if curNode_name=="br" and curNode!="br_{}".format(j) and br_L<br_sample_number:
                                    neigh_train.append(curNode)
                                    neigh_L+=1
                                    br_L+=1
                                elif curNode_name=="rt" and curNode!="rt_{}".format(j) and rt_L<rt_sample_number:
                                    neigh_train.append(curNode)
                                    neigh_L+=1
                                    rt_L+=1
                            
                            #choose from be reported
                            elif curNode_name=="br":
                                curNode=random.choice(self.br_neigh_train_list[curNode_idx])
                                curNode_name,_=_split_name_idx(curNode)
                                if curNode_name=="rp" and curNode!="rp_{}".format(j) and rp_L<rp_sample_number:
                                    neigh_train.append(curNode)
                                    neigh_L+=1
                                    rp_L+=1
                                elif curNode_name=="rt" and curNode!="rt_{}".format(j) and rt_L<rt_sample_number:
                                    neigh_train.append(curNode)
                                    neigh_L+=1
                                    rt_L+=1

                            #choose from report text
                            elif curNode_name=="rt":
                                curNode=random.choice(self.rt_neigh_train_list[curNode_idx])
                                curNode_name,_=_split_name_idx(curNode)
                                if curNode_name=="rp" and curNode!="rp_{}".format(j) and rp_L<rp_sample_number:
                                    neigh_train.append(curNode)
                                    neigh_L+=1
                                    rp_L+=1
                                elif curNode_name=="br" and curNode!="br_{}".format(j) and br_L<br_sample_number:
                                    neigh_train.append(curNode)
                                    neigh_L+=1
                                    br_L+=1
                                elif curNode_name=="rr" and curNode!="rr_{}".format(j) and rr_L<rr_sampele_number:
                                    neigh_train.append(curNode)
                                    neigh_L+=1
                                    rr_L+=1
                                elif curNode_name=="rw" and curNode!="rw_{}".format(j) and rw_L<rw_sample_number:
                                    neigh_train.append(curNode)
                                    neigh_L+=1
                                    rw_L+=1

                            #choose from report reason 
                            elif curNode_name=="rr":
                                curNode=random.choice(self.rr_neigh_train_list[curNode_idx])
                                curNode_name,_=_split_name_idx(curNode)
                                if curNode_name=="rt" and curNode!="rt_{}".format(j) and rt_L<rt_sample_number:
                                    neigh_train.append(curNode)
                                    neigh_L+=1
                                    rt_L+=1

                            #choose from report keywords
                            elif curNode_name=="rw":
                                curNode=random.choice(self.rw_neigh_train_list[curNode_idx])
                                curNode_name,_=_split_name_idx(curNode)
                                if curNode_name=="rt" and curNode!="rt_{}".format(j) and rt_L<rt_sample_number:
                                    neigh_train.append(curNode)
                                    neigh_L+=1
                                    rt_L+=1
                                
                        else:
                            if i==0:
                                curNode="rp_{}".format(j)
                            elif i==1:
                                curNode="br_{}".format(j)
                            elif i==2:
                                curNode="rt_{}".format(j)
                            elif i==3:
                                curNode="rr_{}".format(j)
                            elif i==4:
                                curNode="rw_{}".format(j)
                        
                    # print("rp_{}".format(j),neigh_train)
                if (j + 1)%500 == 0:
                    print("Already walked number for -> {}".format(j+1))
            print("type for -> {} finished walk with {}".format(i,j+1))
            print("finished walked node type for -> {}".format(i))         
        restart_het_random_writer=open(restart_het_random_walk_fp,mode="w",encoding="utf-8")
        for i in range(size_node):
            node_n=node_n_list[i]
            for j in range(node_n):
                if i==0:
                    neigh_train=rp_neigh_random_walk[j]
                    curNode="rp_{}".format(j)
                elif i==1:
                    neigh_train=br_neigh_random_walk[j]
                    curNode="br_{}".format(j)
                elif i==2:
                    neigh_train=rt_neigh_random_walk[j]
                    curNode="rt_{}".format(j)
                elif i==3:
                    neigh_train=rr_neigh_random_walk[j]
                    curNode="rr_{}".format(j)
                elif i==4:
                    neigh_train=rw_neigh_random_walk[j]
                    curNode="rw_{}".format(j)

                if len(neigh_train)>0:
                    neigh_train_join=",".join(neigh_train)
                    write_line="{}:{}\n".format(curNode,neigh_train_join)
                    restart_het_random_writer.write(write_line)
        restart_het_random_writer.close()
        
        

                            
                        
                    

    

    
    
