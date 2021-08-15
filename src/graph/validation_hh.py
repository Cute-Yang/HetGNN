"""
clean the data and keep generate data
discard the row data which don't have the be reported text!
"""
import os
import logging
from log_util import LogFactory
from collections import defaultdict
import json

#the column name of report_ctx
CTX_KEY = "strImpeachSrvParam"
REPORTER_KEY = "strUin"
BE_REPORTED_KEY = "strEvilUin"

LOGGER=LogFactory(
    log_dir="logs",
    log_level=logging.INFO,
    log_prefix="clean_data",
    scope_name="clean_data",
    use_webhook=False,
    use_stream=True,
    file_handler_type="normal",
    timeout=50
)


def make_related_json(src_fp:str,dst_dir:str=None,sep="\001")->None:
    """
    Args:
        src_fp:str,the source file path,should have a header name
        dst_fp:str,the dst file path
        sep:str,the delimiter for src file and dst file
    """
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)
        LOGGER.info()
    src_reader=open(src_fp,mode="r",encoding="utf-8")
    header_names=src_reader.readline().strip().split(sep)

    #the column->idx dict
    header_dict=dict(
        zip(
            header_names,range(len(header_names))
        )
    )
    #fetch the column idx
    ctx_idx=header_dict[CTX_KEY]
    reporter_idx=header_dict[REPORTER_KEY]
    be_reported_idx=header_dict[BE_REPORTED_KEY]
    
    be_reported_dict=defaultdict(list)
    reporter_dict=defaultdict(list)

    for idx,line in enumerate(src_reader):
        line_split=line.strip().split(sep)
        reporter=line_split[reporter_idx]
        be_reported=line_split[be_reported_idx]
        ctx=line_split[ctx_idx]
        reporter_dict[reporter].append(ctx+"->"+be_reported)
        be_reported_dict[be_reported].append(ctx+"->"+reporter)
        if (idx+1)%2000==0:
            LOGGER.info("finished with lines {}".format(idx+1))
    reporter_dict={key:reporter_dict[key] for key in reporter_dict if len(reporter_dict[key])>=2}
    be_reported_dict={key:be_reported_dict[key] for key in be_reported_dict if len(be_reported_dict[key])>=2}
    
    with open("{}/reporter.json".format(dst_dir),"w",encoding="utf-8") as f:
        f.write(
            json.dumps(reporter_dict,ensure_ascii=False)
        )

    with open("{}/be_reported.json".format(dst_dir), "w", encoding="utf-8") as f:
        f.write(
            json.dumps(be_reported_dict, ensure_ascii=False)
        )
    
def make_clean_data(src_fp:str,dst_fp:str,sep="\001")->None:
    """
    if the report ctx not have be_reported,del it
    if the report ctx is too short,del it!
    Args:
        src_fp:str,the source file path
        dst_fp:str,dst file path
    """
    dst_dir_name=os.path.dirname(dst_fp)
    if not os.path.exists(dst_dir_name):
        os.makedirs(dst_dir_name)
        LOGGER.info("Creating {} to save dst file!".format(dst_dir_name))
    dst_writer=open(dst_fp,mode="r",encoding="utf-8")
    
    src_reader=open(src_fp,mode="r",encoding="utf-8")
    header_names=src_reader.readline().strip().split(sep)
    header_names_dict=dict(
        zip(
            header_names,range(len(header_names))
        )
    )
    report_ctx_idx=header_names_dict[CTX_KEY]
    for idx,line in enumerate(src_reader):
    
    

if __name__=="__main__":
    make_related_json(
        src_fp="kf_source_data/kf_report_scene_2021-08-09.txt",
        dst_fp="test/shit.txt"
    )



