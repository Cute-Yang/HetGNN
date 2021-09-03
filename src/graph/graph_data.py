from genericpath import exists
import os
from typing import KeysView
from ..util.auto_log import LogFactory
import sys
import jieba
import numpy as np
from collections import defaultdict
import re
from sklearn.feature_extraction.text import TfidfVectorizer


class Columns(object):
    def __init__(
            self,
            report_ctx_col:str,
            reporter_col:str,
            be_reported_col:str,
            report_reason_col:str,
            report_id_col:str
        ):
        self.report_ctx_col=report_ctx_col
        self.reporter_col=reporter_col
        self.be_reported_col=be_reported_col
        self.report_reason_col=report_reason_col
        self.report_id_col=report_id_col

    def __str__(self):
        return "<Columns name for report data>"
    

class DataNode(object):
    def __init__(self,src_file:str,column_map:Columns,node_idx_dir:str,\
        node_relation_dir:str,delimiter="\001",logger:LogFactory=None,**kwargs):
        self._src_file=src_file
        self._column_map=column_map
        self._node_idx_dir=node_idx_dir
        self._check_dir(node_idx_dir)
        self._node_relation_dir=node_relation_dir
        self._check_dir(node_relation_dir)
        self.logger=logger

        if node_idx_dir==node_relation_dir:
            self.logger.warning("you specify node_idx_dir and node_relation_dir as a same directory->{},which will make you mixed with file!".format(node_idx_dir))
        self.delimiter=delimiter
        self._header_dict=self.get_header_dict()    
        #reporter -> be_reported
        self.RP_BR_dict=defaultdict(list)
        #be_reported -> reporter
        self.BR_RP_dict=defaultdict(list)

        #reporter -> report_ctx
        self.RP_RT_dict=defaultdict(list)
        # be_reported -> report_ctx
        self.BR_RT_dict=defaultdict(list)

        # report_ctx -> reporter
        self.RT_RP_dict=defaultdict(list)
        #report_ctx -> be_reported
        self.RT_BR_dict=defaultdict(list)
        #report_ctx -> report reason
        self.RT_RR_dict=defaultdict(list)
        #report_keywrods -> report_ctx
        self.RT_RW_dict=defaultdict(list)
        
        #report_reason -> report_ctx
        self.RR_RT_dict=defaultdict(list)
        
        #report_keywords -> report_ctx
        self.RW_RT_dict=defaultdict(list)
        
        self.stopwords_file=kwargs.get("stopwords_file",None)
        self.load_stopwords()

        self.userdict_file=kwargs.get("userdict_file",None)
        self.load_userdict()

        self.words_thresh=kwargs.get("word_thresh",5)
        self.silent=kwargs.get("silent",1)

    def load_stopwords(self):
        if self.stopwords_file is not None and os.path.exists(self.stopwords_file):
            self.logger.info("load stopword from {}".format(self.stopwords_file))
            stopwords = [word.strip() for word in open(self.stopwords_file, "r", encoding="utf-8")]
        else:
            self.logger.warning("you did not specify the stopwords file,and the default path is invalid,so the stopwords will be an empty list!")
            stopwords = []
        self.stopwords=stopwords

    def load_userdict(self):
        if self.userdict_file is not None and os.path.exists(self.userdict_file):
            self.logger.info("load user customed word from {}".format(self.userdict_file))
            userdict_words = [word.strip() for word in open(self.userdict_file, "r", encoding="utf-8")]
            for word in userdict_words:
                jieba.add_word(word)
        else:
            self.logger.warning("you did not specify the userdict file,and the default path is invalid,so the userdict words will not be loaded!")

    def get_header_dict(self):
        with open(self.src_file,"r",encoding="utf-8") as f:
            header_line=f.readline().strip().split(self.delimiter)
        size_h=len(header_line)
        header_dict=dict(zip(header_line,range(size_h)))
        return header_dict
    

    def filtered_fetch_relation(self,discared_rows:list)->None:
        """
        make relation file from the src with tfidf discared rows
        Args:
            discared_rows:list->nested with 1 depth
        """
        src_reader=open(self.src_file,"r",encoding="utf-8")
        src_reader.readline()
        try:
            report_ctx_idx = self._header_dict[self._column_map.report_ctx_col]
            reporter_idx = self._header_dict[self._column_map.reporter_col]
            be_reported_idx = self._header_dict[self._column_map.be_reported_col]
            report_reason_idx = self._header_dict[self._column_map.report_reason_col]
            report_id_idx = self._header_dict[self._column_map.report_id_col]
        except:
            self.logger.error("can not filed name in first line! your header_dict is {}".format(
                self._header_dict), exc_info=True)
            sys.exit(1)
        cnt=0
        reporter_set=set()
        be_reported_set=set()
        report_reason_set=set()
        report_id_list=[]
        for idx, line in enumerate(src_reader):
            line_split = line.strip().split(self.delimiter)
            if len(line_split)!=len(self._header_dict) and self.silent==0:
                self.logger.warning(
                    "error bad lines at {},the size_line not equal to size_header".format(
                        idx
                    )
                )
                continue
            report_ctx=line_split[report_ctx_idx]
            if not self._check_report_ctx(report_ctx):
                continue
                #this is very important
                #fetch the report data
            cut_words = self.cut_report_ctx(report_ctx)

            if len(cut_words) <= self.words_thresh:
                # print(cut_words,report_ctx)
                continue
            if cnt in discared_rows:
                #if you not remove this value,you will continue forever!
                discared_rows.remove(cnt)
                continue
        
            reporter=line_split[reporter_idx]
            be_reported=line_split[be_reported_idx]
            report_reason=line_split[report_reason_idx]
            report_id=line_split[report_id_idx]
            
            report_id_list.append(report_id)

            reporter_set.add(reporter)
            be_reported_set.add(be_reported)
            report_reason_set.add(report_reason)


            self.RP_BR_dict[reporter].append(be_reported)
            self.RP_RT_dict[reporter].append(cnt)

            self.BR_RP_dict[be_reported].append(reporter)
            self.BR_RT_dict[be_reported].append(cnt)

            self.RT_RP_dict[cnt].append(reporter)
            self.RT_BR_dict[cnt].append(be_reported)
            self.RT_RR_dict[cnt].append(report_reason)

            self.RR_RT_dict[report_reason].append(cnt)
            cnt+=1
        print("second stage -> {}".format(cnt))
        reporter_2_idx=dict(
            zip(reporter_set,range(len(reporter_set)))
        )
        be_reported_2_idx=dict(
            zip(be_reported_set,range(len(be_reported_set)))
        )
        report_reason_2_idx=dict(
            zip(report_reason_set,range(len(report_reason_set)))
        )
        report_id_2_idx=dict(
            zip(report_id_list,range(len(report_id_list)))
        )
        self.RP_BR_dict = self._convert_2_idx(self.RP_BR_dict, reporter_2_idx, be_reported_2_idx, convert_type="all")
        self.RP_RT_dict = self._convert_2_idx(self.RP_RT_dict, key_convert_map=reporter_2_idx, convert_type="key")
        self.BR_RP_dict = self._convert_2_idx(self.BR_RP_dict, be_reported_2_idx, reporter_2_idx, convert_type="all")
        self.BR_RT_dict = self._convert_2_idx(self.BR_RT_dict, key_convert_map=be_reported_2_idx, convert_type="key")
        self.RT_BR_dict = self._convert_2_idx(self.RT_BR_dict, value_convert_map=be_reported_2_idx, convert_type="value")
        self.RT_RP_dict = self._convert_2_idx(self.RT_RP_dict, value_convert_map=reporter_2_idx, convert_type="value")
        self.RT_RR_dict = self._convert_2_idx(self.RT_RR_dict, value_convert_map=report_reason_2_idx, convert_type="value")
        self.RR_RT_dict = self._convert_2_idx(self.RR_RT_dict, key_convert_map=report_reason_2_idx, convert_type="key")
            
        #save node idx
        report_id_node_fp = os.path.join(self._node_idx_dir, "report_id_node.txt")
        self._save_node_idx(report_id_2_idx, report_id_node_fp)

        reporter_node_fp = os.path.join(
            self._node_idx_dir, "reporter_node.txt")
        self._save_node_idx(reporter_2_idx, reporter_node_fp)

        be_reported_node_fp = os.path.join(
            self._node_idx_dir, "be_reported_node.txt")
        self._save_node_idx(be_reported_2_idx, be_reported_node_fp)

        report_reason_node_fp = os.path.join(
            self._node_idx_dir, "report_reason_node.txt")
        self._save_node_idx(report_reason_2_idx, report_reason_node_fp)
    

    def fetch_relation(self):
        src_reader=open(self.src_file,"r",encoding="utf-8")
        src_reader.readline()
        
        try:
            report_ctx_idx=self._header_dict[self._column_map.report_ctx_col]
        except:
            self.logger.error("can not filed name in first line! your header_dict is {}".format(self._header_dict),exc_info=True)
            sys.exit(1)
        
        words_list=[]
        cnt=0
        for idx,line in enumerate(src_reader):
            line_split=line.strip().split(self.delimiter)
            if len(line_split)!=len(self._header_dict) and self.silent==0:
                self.logger.warning(
                    "error bad lines at {},the size_line not equal to size_heaer".format(
                        idx
                    )
                )
                continue
            report_ctx=line_split[report_ctx_idx]
            if not self._check_report_ctx(report_ctx):
                continue

            #fetch the report data
            cut_words=self.cut_report_ctx(report_ctx)
            if len(cut_words)<=self.words_thresh:
                continue
            words_list.append(cut_words)
            cnt+=1
        print("first stage->{}".format(cnt))
        
        #keywords from tfidf
        words_list_corpus=[" ".join(item) for item in words_list]
        report_keywords_2_idx,report_words_nestd_list,discarded_rows=self._make_words_tfidf(words_list_corpus,min_df=5,max_df=0.5)
        print(discarded_rows)
        for report_idx,report_words in enumerate(report_words_nestd_list,start=0):
            self.RT_RW_dict[report_idx]=report_words
            for word_idx in report_words:
                self.RW_RT_dict[word_idx].append(report_idx)

        self.filtered_fetch_relation(discared_rows=discarded_rows)
    
        report_keyword_node_fp=os.path.join(self._node_idx_dir,"report_keyword_node.txt")
        self._save_node_idx(report_keywords_2_idx,report_keyword_node_fp)
        
        
        #save relation node 
        
        #for reporter
        RP_BR_relation_fp=os.path.join(self._node_relation_dir,"rp_br_train.txt")
        self._save_relation_node(self.RP_BR_dict,RP_BR_relation_fp)
        RP_RT_relation_fp=os.path.join(self._node_relation_dir,"rp_rt_train.txt")
        self._save_relation_node(self.RP_BR_dict,RP_RT_relation_fp)
        
        #for be reported
        BR_RP_relation_fp=os.path.join(self._node_relation_dir,"br_rp_train.txt")
        self._save_relation_node(self.BR_RP_dict,BR_RP_relation_fp)
        BR_RT_relation_fp=os.path.join(self._node_relation_dir,"br_rt_train.txt")
        self._save_relation_node(self.BR_RT_dict,BR_RT_relation_fp)
        
        #for report ctxw
        RT_RP_relation_fp=os.path.join(self._node_relation_dir,"rt_rp_train.txt")
        self._save_relation_node(self.RT_RP_dict,RT_RP_relation_fp)
        RT_BR_relation_fp=os.path.join(self._node_relation_dir,"rt_br_train.txt")
        self._save_relation_node(self.RT_BR_dict,RT_BR_relation_fp)
        RT_RR_relation_fp=os.path.join(self._node_relation_dir,"rt_rr_train.txt")
        self._save_relation_node(self.RT_RR_dict,RT_RR_relation_fp)
        RT_RW_relation_fp=os.path.join(self._node_relation_dir,"rt_rw_train.txt")
        self._save_relation_node(self.RT_RW_dict,RT_RW_relation_fp)

        #for report reason
        RR_RT_relation_fp=os.path.join(self._node_relation_dir,"rr_rt_train.txt")
        self._save_relation_node(self.RR_RT_dict,RR_RT_relation_fp)
            
        #for report keywords
        RW_RT_relation_fp=os.path.join(self._node_relation_dir,"rw_rt_train.txt")
        self._save_relation_node(self.RW_RT_dict,RW_RT_relation_fp)
            
    
    def _save_node_idx(self,node_idx_dict:dict,save_file:str)->None:
        node_tuple=[(key,value) for key,value in node_idx_dict.items()]
        node_tuple_sorted=sorted(
            node_tuple,
            key=lambda x:x[1]
        )
        self.logger.info("making node data -> {}".format(save_file))
        with open(save_file,"w",encoding="utf-8") as f:
            for node,idx in node_tuple_sorted:
                wc="{}:{}\n".format(node,idx)
                f.write(wc)
    
    def _save_relation_node(self,relation_dict:dict,save_file:str,delimiter=",")->None:
        self.logger.info("making relation data -> {}".format(save_file))
        relation_tuple=[(key,value) for key,value in relation_dict.items()]
        relation_tuple_sorted=sorted(
            relation_tuple,
            key=lambda x:x[0]
        )
        with open(save_file,"w",encoding="utf-8") as f:
            for k,v_list in relation_tuple_sorted:
                v_list=[str(v) for v in v_list]
                wc="{}:{}\n".format(k,delimiter.join(v_list))
                f.write(wc)

    def _convert_2_idx(self,src:dict,key_convert_map=None,value_convert_map=None,convert_type="all")->dict:
        """
        Args:
            src:the src should be like {key:[v1,v2,v3]}
            key_convert_map:key -> idx map
            value_convert_map:value -> idx map
            convert_type:valid mod is all,key,value,if others,raise ValueError
        Returns:
            new_map:a new map id->id
        """
        if convert_type=="all":
            if key_convert_map is None or value_convert_map is None:
                raise ValueError("essential param key_convert_map/value_convert_map is missing.....")
            new_map={}
            for key in src:
                key_idx=key_convert_map[key]
                value_list=src[key]
                value_list=[value_convert_map[v] for v in value_list]
                new_map[key_idx]=value_list

        elif convert_type=="value":
            if value_convert_map is None:
                raise ValueError("essential param value_convert_map is missing.....")
            for key in src:
                value_list=src[key]
                value_list=[value_convert_map[v] for v in value_list]
                src[key]=value_list
                new_map=src
        elif convert_type=="key":
            if key_convert_map is None:
                raise ValueError("essential param key_convert_map is missing.....")
            new_map={}
            for key in src:
                key_idx=key_convert_map[key]
                new_map[key_idx]=src[key]
        else:
            raise ValueError("valid convert type is ({},{},{}),but you specify {}".format(
                "all",
                "key",
                "value",
                convert_type
            ))
        return new_map
    

    def cut_report_ctx(self,report_ctx:str,report_ctx_delimiter="|")->list:
        words=[]
        report_ctx_split=report_ctx.split(report_ctx_delimiter)[1:]
        for single_report_ctx in report_ctx_split:
            c_single_report_ctx=self._clean_single_report_rtx(single_report_ctx)
            if len(c_single_report_ctx)<=5:
                continue
            cw=jieba.lcut(c_single_report_ctx)
            cw=[w for w in cw if w not in self.stopwords]
            words.extend(cw)
        return words
        
    
    def _clean_single_report_rtx(self,single_report_ctx:str)->str:
        single_report_ctx_split=single_report_ctx.split(":",1)
        if len(single_report_ctx_split)==2:
            single_report_ctx_=single_report_ctx_split[1]
        else:
            return ""
    
        r1 = re.compile(
            r"(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]")
        r2 = re.compile(
            r"50108.njc.svp.tencent-cloud.com/[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]")
        r3 = re.compile(
            r"<[' ']{0,1}appmsg start[' ']{0,1}>title:微信红包 (des){0,1}:我给你发了一个红包，赶紧去拆! .*info:<[' ']{0,1}appmsg end[' ']{0,1}>")
        r4 = re.compile(
            r"<[' ']{0,1}appmsg start[' ']{0,1}>title:微信转账.*? (des){0,1}:收到转账\d{1,}\.\d{0,2}元。如需收钱，请点此升级至最新版本 .*info:<[' ']{0,1}appmsg end[' ']{0,1}>")

        r5 = re.compile(
            "<[' ']{0,1}appmsg start[' ']{0,1}>title:微信转账.* (des){0,1}:.*?向你转账\d{1,}\.\d{0,2}元。如需收钱，请点击升级至最新版本 .*info:<[' ']{0,1}appmsg end[' ']{0,1}>|向你转账\d{1,}\.\d{0.2}元")
        r6 = re.compile(
            r"我.*现在我们可以开始聊天了|@.* |@所有人|对方请求添加你为好友|我是.*?|appmsg|start|des|end|title|info")
        #remove the url link ,but we should keep the text
        r7 = re.compile(r"<appmsg start>title:.*?<appmsg end>")
        r8 = re.compile(r"\[.*?\]")
        r9 = re.compile(r"[^a-zA-Z\u4e00-\u9fa5]")
        for r in (r1,r2,r3,r4,r5,r6,r7,r8,r9):
            single_report_ctx_=re.sub(r,"",single_report_ctx_)

        exclude_r = r"^[a-zA-Z0-9 ]+$"
        if re.match(exclude_r, single_report_ctx_):
            return ""
        
        single_report_ctx_=single_report_ctx_.lower()
        return single_report_ctx_
    

    def _check_report_ctx(self,report_ctx:str,delimiter="|")->bool:
        report_ctx_split=report_ctx.split(delimiter)[1:]
        for report_ctx_ in report_ctx_split:
            essential_=("被举报人","群主被举报人","其他人","群主其他人")
            for et in essential_:
                if report_ctx_.startswith(et):
                    return True
        return False    


    def _make_words_tfidf(self,words_list:list,min_df,max_df,top_k=10)->tuple:
        """
        fetch the tfidf matrix or words_list,and return the filted row_idx 
        Args:
            words_list:a two depth nested words_list
            min_df:
        """
        discared_rows=[]
        vecotorzier=TfidfVectorizer(min_df=min_df,max_df=max_df)
        tfidf=vecotorzier.fit_transform(words_list)
        #the the words list
        tfidf_words=vecotorzier.get_feature_names()
        index_2_words=dict(
            zip(
                range(len(tfidf_words)),tfidf_words
            )
        )
        
        #find the tfidf value top_k and words
        weight=tfidf.toarray()
        index_sort=np.argsort(-weight,axis=1).astype(np.int32)
        top_k_index_sort:np.ndarray=index_sort[:,:top_k].copy()
        #free memory
        del index_sort
        keywords_nested_list=[]
        keywords_list=set()
        for row_idx,index_vlaues in enumerate(top_k_index_sort):
            kw_index=[index for index in index_vlaues if weight[row_idx,index]>0]
            if not kw_index:
                discared_rows.append(row_idx)
                continue
            keywords=[index_2_words[index] for index in kw_index]
            keywords_nested_list.append(keywords)
            keywords_list.update(keywords)
        filter_words_2_index=dict(
            zip(
                keywords_list,range(len(keywords_list))
            )
        )
        keywords_nested_list=[[filter_words_2_index[word] for word in words] for words in keywords_nested_list]
        return filter_words_2_index,keywords_nested_list,discared_rows
    
    

    @property
    def src_file(self):
        return self._src_file

    def _check_dir(self,dir_name):
        if not os.path.exists(dir_name):
            info="creating directory {} to save data".format(os.path.abspath(dir_name))
            self.logger.info(info)
            os.makedirs(dir_name)
        
    
    
if __name__=="__main__":
    column_map=Columns(
        report_ctx_col="strImpeachSrvParam",
        be_reported_col="strEvilUin",
        reporter_col="strUin",
        report_reason_col="ullImpeachReason",
        report_id_col="id"
    )
    
    het_data=DataNode(
        src_file="kf_source_data",
        column_map=column_map,
        node_idx_dir="data/node_data",
        node_relation_dir="data/graph_data"
    )
    #fetch the relation data
    het_data.fetch_relation()
