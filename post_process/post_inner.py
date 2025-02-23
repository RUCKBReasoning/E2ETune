import copy
import json
import jsonlines

features = ['xact_commit', 'xact_rollback', 'blks_read', 'blks_hit', 'tup_returned', 'tup_fetched', 'tup_inserted', 'tup_updated', 'tup_deleted', 'conflicts', 'temp_files', 'temp_bytes']

data = []
# with open('testjson.json') as f:
#     data = [json.load(f)]
with jsonlines.open('../train4.jsonl') as f:
    for line in f:
        data.append(line)

all_results = []
for line in data:
    line2 = copy.deepcopy(line)
    print(line2)
    del line2['before']
    del line2['after']
    before = line['before']['db_info']
    after = line['after']['db_info']
    feature_des = ''
    for feature in features:
        delta = after[feature] - before[feature]
        if feature == 'temp_bytes':
            if delta > 1024 and delta < 1024*1024:
                delta = f'{int(delta/1024)}KB'
            elif delta >= 1024*1024 and delta < 1024*1024*1024:
                delta = f'{int(delta/(1024*1024))}MB'
            elif delta >= 1024*1024*1024:
                delta = f'{int(delta/(1024*1024*1024))}GB'
            else: delta = f'{delta} bytes'
            
        else:
            if delta > 1000 and delta < 1000000:
                delta = f'{int(delta/1000)}k'
            elif delta >= 1000000:
                delta = f'{int(delta/1000000)} million' 
            else: delta = str(delta)
        feature_des += f'{feature}: {delta}; '
    line2['inner_features'] = feature_des
    all_results.append(line2)

with open('train4.json', 'w') as w:
    json.dump(all_results, w, indent=4)

