import ast


def read_data_from_file(file_path):
    with open(file_path, 'r') as lines:
        for line in lines:
            line = line.strip().replace('true', 'True').replace('false', 'False')
            # not using json.loads since string would be converted to unicode
            # which can not be converted back when producing messages
            json_obj = ast.literal_eval(line)
            yield json_obj


def merge_multiple_files(file_list):
    result = dict()
    for fpath in file_list:
        for obj in read_data_from_file(fpath):
            if obj['topic'] not in result:
                result[obj['topic']] = obj['data']
            else:
                result[obj['topic']].extend(obj['data'])
    return result


def merge_dicts(dict_list):
    result = {}
    for dictionary in dict_list:
        result.update(dictionary)
    return result
