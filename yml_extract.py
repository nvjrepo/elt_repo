import yaml

def etract_variable_yml_dict(dictionary,file_name='base_vn_token'):
    a_yaml_file = open("credentials/"+file_name+".yml") 
    parsed_yaml_file = yaml.load(a_yaml_file, Loader=yaml.FullLoader)
    token=parsed_yaml_file[dictionary]
    return token

def etract_variable_yml_string(dictionary_1,dictionary_2,file_name='base_vn_token'):
    a_yaml_file = open("credentials/"+file_name+".yml") 
    parsed_yaml_file = yaml.load(a_yaml_file, Loader=yaml.FullLoader)
    token_dict=parsed_yaml_file[dictionary_1]
    token=token_dict[dictionary_2]
    return token
