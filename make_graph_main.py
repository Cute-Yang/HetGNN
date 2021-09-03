from src.graph.graph_data import (
    Columns,
    DataNode,

)
from src.util.auto_log import LogFactory
import argparse


arg_parer = argparse.ArgumentParser("graph data fetch")
arg_parer.add_argument("--stopwords", type=str,
                       default="assits/stopwords.txt", help="stopwords file path...")
arg_parer.add_argument("--userdict", type=str, default="assits/userdict.txt",
                       help="person specify words,for some customized or rare word...")
arg_parer.add_argument("--src_file", type=str,
                       default="filter_file/kf_report_scene_2021-08-09_tiny.txt", help="")
arg_parer.add_argument("--node_idx_dir", type=str, default="data/node_data",
                       help="the directory to save node and node idx map...")
arg_parer.add_argument("--node_relation_dir", type=str,
                       default="data/graph_data", help="the directory to save node relation...")

logger = LogFactory(
    log_dir="logs",
    scope_name="graph data",
    use_webhook=False,
    file_handler_type="normal",
    log_prefix="graph_data.log"
)


args = arg_parer.parse_args()
stopwords_file = args.stopwords
userdict_file = args.userdict
src_file = args.src_file
node_idx_dir = args.node_idx_dir
node_relation_dir = args.node_relation_dir

if __name__=="__main__":
    column_map = Columns(
        report_ctx_col="strImpeachSrvParam",
        be_reported_col="strEvilUin",
        reporter_col="strUin",
        report_reason_col="ullImpeachReason",
        report_id_col="id"
    )

    het_data=DataNode(
        src_file=src_file,
        column_map=column_map,
        node_idx_dir=node_idx_dir,
        node_relation_dir=node_relation_dir,
        logger=logger,
        stopwords_file=stopwords_file,
        userdict_file=userdict_file
    )
    #fetch the relation data
    het_data.fetch_relation()
