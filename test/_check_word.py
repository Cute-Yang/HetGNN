word_path="data/graph_data/rt_rw_train.txt"
with open(word_path,"r",encoding="utf-8") as f:
    for line in f:
        line_split=line.strip().split(":",1)
        if line_split[1]=="":
            print(line)