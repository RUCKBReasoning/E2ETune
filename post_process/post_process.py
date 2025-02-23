import re
import json
import copy
import jsonlines
import random

sample_more = False
path1 = '../record/offine_record.jsonl'
path2 = '../record/surrogate_result.jsonl'
all_data = []

testset = ['_movie_platform_21.wg', '_world_development_indicators_11.wg', 'language_corpus_3.wg', 'talkingdata_13.wg', '_movie_platform_12.wg', 'talkingdata_5.wg', 'bike_share_1_39.wg', 'bike_share_1_10.wg', '54.wg', 'language_corpus_24.wg', '_movie_platform_14.wg', '_codebase_comments_11.wg', '40.wg', 'language_corpus_29.wg', 'talkingdata_4.wg', 'talkingdata_10.wg', '_donor_11.wg', '3.wg', 'talkingdata_1.wg', 'language_corpus_25.wg', 'talkingdata_7.wg', 'talkingdata_10.wg', '_retails_5.wg', 'language_corpus_9.wg', '_movie_platform_33.wg', 'talkingdata_15.wg', '24.wg', '6.wg', '_retails_9.wg', 'language_corpus_26.wg', 'talkingdata_3.wg', '54.wg', 'language_corpus_28.wg', 'bike_share_1_47.wg', 'bike_share_1_49.wg', '52.wg', 'language_corpus_32.wg', '_movie_platform_16.wg', 'language_corpus_31.wg', '_retails_6.wg']

default = [16.0, 3.0, 200.0, 2048.0, 4096.0, 0.1, 50.0, 600.0, 2.0, -1.0, 0.2, 50.0, 0.0, 2000.0, 64.0, 100.0, 2.0, 0.5, 32.0, 900.0, 0.0, 5.0, 0.1, 1000.0, 100.0, 16384.0, 1.0, 8.0, 5.0, 0.0, 0.0, 0.0, 12.0, 8.0, 16384.0, 128.0, -1.0, 0.0, 200.0, 20.0, 1.0, 10.0, 200.0, 65536.0]
knob_details = json.load(open('../knob_config/knob_config.json'))


default_config = {}
default_config2 = {}
for i, knob in enumerate(knob_details):
    default_config[knob] = knob_details[knob]['default']
    default_config2[knob] = float(default[i])
# print(default_config)

def test_same_config(config1, config2):
    same = True
    for knob in knob_details:
        if knob not in config1.keys() or knob not in config2.keys():
            return True
        if config1[knob] != config2[knob]:
            same = False
    return same


def read_data(path):
    data = {}
    with jsonlines.open(path, 'r') as f:
        for record in f:
            x = {}
            # y.append(-record['tps'])
            for key in record.keys():
                if key == 'y' or key == 'workload' or key == 'tps' or key == 'inner_metrics': continue
                else:
                    x[key] = record[key]
            if not test_same_config(x, default_config) and not test_same_config(x, default_config2):
                workload = record['workload'].split('SuperWG/res/gpt_workloads/')[1]
                if workload in data.keys(): data[workload][record['tps']] = [x, record['inner_metrics']]
                else: data[workload] = {record['tps']: [x, record['inner_metrics']]}
    return data


# with open('training_data.json') as f:
#     all_data = json.load(f)
if sample_more:
    databases = ['_aminer_simplified', '_donor', '_codebase_comments', '_movie_platform', '_retails', '_world_development_indicators', 'bike_share_1', 'language_corpus', 'talkingdata', 'tpch']
    all_length = 0
    for database in databases:
        records = read_data(f'../offline_sample/offline_sample_{database}.jsonl')
        print(database, len(records))
        all_length += len(records)
        features = json.load(open(f'../SuperWG/feature/{database}.json'))
        for key in records:
            if key in testset:
                continue
            # if database != 'tpch':
            #     idx = key.split(f'{database}_')[1]
            #     idx = int(idx.split('.wg')[0])
                # if idx > 30:
                #     continue
            tmp = records[key]
            # r = max([i[1] for i in tmp]) 
            # l = min([i[1] for i in tmp])
            pre_config = default_config
            count = 0
            sorted_tmp = sorted(tmp.keys())
            data = {}
            data['workload_name'] = key
            data['database'] = database
            data['workload'] = open(f"../SuperWG/res/gpt_workloads/{key}").read()
            if f'SuperWG/res/gpt_workloads/{key}' not in features.keys():
                # print(f'SuperWG/res/gpt_workloads/{key}')
                continue
            else: data['feature'] = features[f'SuperWG/res/gpt_workloads/{key}']
            if len(sorted_tmp) < 10:
                continue
            elif len(sorted_tmp) < 100:
                idxes = [0, int(len(sorted_tmp)/3), int(len(sorted_tmp)/3*2), len(sorted_tmp)-1]
            else:
                idxes = [0, int(len(sorted_tmp)/4), int(len(sorted_tmp)/2), int(len(sorted_tmp)/4*3), len(sorted_tmp)-1]
            for idx in idxes:
                tps = sorted_tmp[idx]
                x = tmp[tps][0]
                if test_same_config(pre_config, x):
                    print('skip', idx)
                    continue
                d = copy.deepcopy(data)
                d['pre_config'] = pre_config
                d['best_config'] = x
                d['inner_metrics'] = tmp[tps][1]
                pre_config = x
                all_data.append(d)

        print(len(all_data))
            # for tps in sorted_tmp:
            #     data = {}
            #     data['workload_name'] = key
            #     data['database'] = database
            #     data['workload'] = open(f"../SuperWG/res/gpt_workloads/{key}").read()
            #     if f'SuperWG/res/gpt_workloads/{key}' not in features.keys():
            #         # print(f'SuperWG/res/gpt_workloads/{key}')
            #         break
            #     else: data['feature'] = features[f'SuperWG/res/gpt_workloads/{key}']
            #     data['inner_metrics'] = tmp[tps][2]
            #     if record[1] - l > -0.001*l:
            #         if test_same_config(pre_config, record[0]) or test_same_config(default_config, record[0]):
            #             continue
            #         if count <= 3 or record[1] == r:
            #             l = record[1]
            #             count += 1
            #             data['pre_config'] = pre_config
            #             data['best_config'] = record[0]
            #             pre_config = record[0]
            #             all_data.append(data)
            #             if record[1] == r:
            #                 print(count, record[1])
            #                 break

else: 
    with jsonlines.open(path2) as f:
        for line in f:
            data = {}
            name = line['workload']
            data['workload_name'] = name
            workload = open(f"../SuperWG/res/gpt_workloads/{name}").read()
            database = re.split('\d+.wg$', name)[0]
            if len(database) == 0:
                data['database'] = 'tpch'
            elif database == 'job': continue
            else: data['database'] = database.rstrip('_')
            database = data['database']
            features = json.load(open(f'../SuperWG/feature/{database}.json'))
            if f'SuperWG/res/gpt_workloads/{name}' not in features.keys():
                print(f'SuperWG/res/gpt_workloads/{name}')
                continue
            else: data['feature'] = features[f'SuperWG/res/gpt_workloads/{name}']
            data['workload'] = workload
            data['inner_metrics'] = line['inner']
            data['pre_config'] = default_config
            # data['inner_metrics'] = inner_tmp
            data['best_config'] = line['best_config']
            all_data.append(data)
            # break


print(len(all_data))
# all_test = []
# for i in range(40):
#     idx = random.randint(0, len(all_data)-1)
#     test = all_data[idx]
#     all_data.pop(idx)
#     all_test.append(test)

result = json.dumps(all_data, indent=4)
print(len(all_data))
with open('train6.json', 'a') as w:
    w.write(result)

# result = json.dumps(all_test, indent=4)
# with open('test3.json', 'a') as w:
#     w.write(result)