import glob
import json



read_files = glob.glob("*.txt")
for f in read_files:
    with open (f, 'rb') as input:
        contents = input.read()
        record = [f, contents]
        print json.dumps(record)
    
