import os


def fetch_header_dict(src_path, line_sep="\001") -> dict:
    """
    fetch the field dict from source file
    so the first line must be your field name

    Args:
        src:path:str->source file path
        line_sep:str->the sep of line ctx,default value is \001
    """
    with open(src_path, mode="r", encoding="utf-8") as f:
        header_line = f.readline().strip()
        header_split = header_line.split(line_sep)
        header_dict = dict(zip(header_split, range(len(header_split))))
    return header_dict


def filter_repeat(src_path: str, dst_path: str, header_dict: dict, ignore=True, line_sep="\001") -> None:
    """
    Args:
        src_path:str->the file path
        dst_path:str-> the dst path
        header_dict:dict,column->idx map
        ignore:bool,if True,we will skip the first line
        line_sep:the delimiter of line ctx,default value is '\001'
    """
    save_dirname = os.path.dirname(dst_path)
    if not os.path.exists(save_dirname):
        os.makedirs(save_dirname)

    reporter_field_key = "strUin"
    # report_ctx_key = "strImpeachSrvParama"
    be_reported_field_key = "strEvilUin"
    report_time_key = "add_time"
    try:
        reporter_idx = header_dict[reporter_field_key]
        # report_ctx_idx = header_dict[report_ctx_key]
        be_report_idx = header_dict[be_reported_field_key]
        report_time_idx = header_dict[report_time_key]
    except KeyError:
        raise

    #use the reporter_reporttime as the key to filter content
    writer = open(dst_path, mode="w", encoding="utf-8")
    #to filter the reporter and time
    chatroom_set = set()
    #to filter the reporter and be_reported
    person_set = set()
    filter_count = 0
    total_count = 0
    with open(src_path, mode="r", encoding="utf-8") as f:
        if ignore:
            line = f.readline()
            writer.write(line)
        for line in f:
            total_count += 1
            line_split = line.strip().split(line_sep)
            reporter = line_split[reporter_idx]
            # report_ctx=line_split[report_ctx_idx]
            be_reported = line_split[be_report_idx]
            report_time = line_split[report_time_idx]
            unique_key_chatroom = "{}_{}".format(reporter, report_time)
            unique_key_person = "{}_{}".format(reporter, be_reported)

            flag_1 = (unique_key_chatroom in chatroom_set)
            flag_2 = (unique_key_person in person_set)

            if not flag_1:
                chatroom_set.add(unique_key_chatroom)
            if not flag_2:
                person_set.add(unique_key_person)

            if flag_1 or flag_2:
                continue
            writer.write(line)
            filter_count += 1
    info = "total_size is {},filter_size is {}".format(
        total_count, filter_count)
    print(info)


if __name__=="__main__":
    source_data_path = "kf_source_data/kf_report_scene_2021-08-09.txt"
    dst_data_path = "filter_file/kf_report_scene_2021-08-09.txt"
    header_dict = fetch_header_dict(source_data_path)
    filter_repeat(source_data_path, dst_data_path, header_dict=header_dict)
