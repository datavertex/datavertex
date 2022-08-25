import json
import re
from jsonpath_ng.ext import parse
from glob import glob
from os import path

def find_notebook_in_local_dir(location, notebook_id):
    for file in glob(path.join(location, '*.ipynb')):
        #print(file)
        id = extract_notebook_id(file)
        if id == notebook_id:
            return file
    raise KeyError(notebook_id)

def extract_notebook_id(filename):
    with open(filename) as f:
        data = json.load(f)
        
    jspath = parse('$.cells[?(cell_type == "code")]')
    regex = re.compile(r'^[ ]*[A-Za-z0-9_]+[ ]*=[ ]*Notebook\([ ]*[\'"](?P<id>[A-Za-z0-9_/-]+)[\'"][ ]*\)')
    
    for cell in jspath.find(data):
        code = cell.value['source']
        
        for line in code:
            match = regex.search(line)
            if (match):
                id = regex.search(line).group('id')
                if id is not None:
                    return id
    return None