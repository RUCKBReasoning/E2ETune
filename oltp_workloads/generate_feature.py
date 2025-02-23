import json
import numpy as np

# benchmarks = ['smallbank', 'twitter', 'wikipedia', 'ycsb']
benchmarks = ['tpcc']

for bench in benchmarks:

    mapper = json.load(open(f'{bench}/mapper.json'))
    meta = json.load(open(f'{bench}/meta_feature.json'))

    result = {}
    for key in mapper.keys():
        weights = mapper[key]
        # features = [0 for i in meta[f'SuperWG/res/oltp_workloads/{bench}3.wg']]
        features = [0 for i in meta['1']]
        for i, w in enumerate(weights):
            tmp = meta[str(i+1)]
            # tmp = meta[f'SuperWG/res/oltp_workloads/{bench}{i}.wg']
            tmp = list(map(lambda x : x * w / 100, tmp))
            features = np.sum([features, tmp], axis=0).tolist()
        features += weights
        result[key] = features[1:]

    with open(f'{bench}.json', 'w') as w:
        json.dump(result, w, indent=4)