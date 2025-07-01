import csv
import time
import json
import math
from numpy import NaN
import shutil
import joblib

from Database import Database
import utils
from config import parse_config
from stress_testing_tool import stress_testing_tool
from tuner import tuner
from knob_config.parse_knob_config import get_knobs

import argparse
import jsonlines

# format: {workload: [knobs]}
# with open('model_res.json', 'r') as f:
#     model_results = json.load(f)


default = [16.0, 3.0, 200.0, 2048.0, 8192.0, 0.1, 50.0, 600.0, 2.0, -1.0, 0.2, 50.0, 0.0, 2000.0, 64.0, 100.0, 2.0, 0.5, 32.0, 900.0, 0.0, 5.0, 0.1, 1000.0, 100.0, 16384.0, 1.0, 8.0, 5.0, 0.0, 0.0, 0.0, 12.0, 8.0, 16384.0, 128.0, -1.0, 0.0, 200.0, 20.0, 1.0, 10.0, 200.0, 65536.0]

workload_map = []

label_mapper_s1 = {
    '00% to 10%': 0,
    '10% to 20%': 1,
    '20% to 30%': 2,
    '30% to 40%': 3,
    '40% to 50%': 4,
    '50% to 60%': 5,
    '60% to 70%': 6,
    '70% to 80%': 7,
    '80% to 90%': 8,
    '90% to 100%': 9
}

label_mapper_s2 = {
    'extremely low': 0,
    'very low': 1,
    'low': 2, 
    'slightly low': 3,
    'middle': 4,
    'slightly high': 5,
    'high': 6,
    'very high': 7,
    'extremyly high': 8
}

select = False
feature_path = '/home/u2023103707/opengauss_tuning/SuperWG/feature/tpch.json'
model_path = '/home/u2023103707/opengauss_tuning/surrogate/tpch.pkl'

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='10.10.252.11', help='the database host')
    parser.add_argument('--datapath', type=str, default='/fs/fast/u2023103707/tpch242', help='the database host')
    parser.add_argument('--workload', type=str, default='job_11.wg', help='the database host')
    cmd = parser.parse_args()

    args = parse_config.parse_args("config/config.ini")
    args['ssh_config']['host'] = cmd.host
    args['database_config']['data_path'] = cmd.datapath

    model_results = json.load(open('test/test_res6.json'))
    model_results_2db = model_results[0]['gpt']
    model_results_4db = model_results[0]['gpt']
    model_results = model_results[0]['all_outputs']

    label_data = {}
    with jsonlines.open('record/offine_record.jsonl') as f:
        for line in f:
            label_data[line['workload']] = line['best_config']
    
    print(label_data)
    model_config = {}
    workload = cmd.workload
    if select:
        best_config = {}
        best_perf = 0
        for s1 in model_results[0:8]:
            model = joblib.load(model_path)
            features = json.load(open(feature_path))
            feature = features[f'SuperWG/{workload}']
            
            x = []
            knobs_detail = get_knobs('knob_config/knob_config2.json')
            for index, knob in enumerate(knobs_detail):
                detail = knobs_detail[knob]
                length = (detail['max'] - detail['min']) / 10
                if length == 0: 
                    continue
                else:
                    s1_value = label_mapper_s1[s1[knob]]
                    x.append(length * s1_value + length / 2)
            print(len(x))
            x += feature
            score = model.predict([x])[0] * 10
            print(x, score)
            if score > best_perf:
                best_perf = score
                best_config = s1
        model_results = [s1]
        print(s1)

    else: 
        model_results_2db = model_results_2db[0:8]
        model_results_4db = model_results_4db[0:8]
    for s1, s2 in zip(model_results, model_results):
        # workload = line['workload']
        args['benchmark_config']['workload_path'] = 'SuperWG/' + workload
        args['benchmark_config']['tool'] = 'dwg'
        database = Database(args, 'knob_config/knob_config2.json')
        logger = utils.get_logger(args['tuning_config']['log_path'])
        sample = args['tuning_config']['finetune_sample']
        stt = stress_testing_tool(args, database, logger, sample)
        knobs_detail = get_knobs('knob_config/knob_config2.json')
        print(f'test workload {workload}')

        point_s1 = {}
        point_s2 = {}
        point_s3 = {}
        default_point = {}
        s2_map = json.load(open('post_process/map_s2.json'))
        for index, knob in enumerate(knobs_detail):
            detail = knobs_detail[knob]
            length = (detail['max'] - detail['min']) / 10
            default_point[knob] = float(detail['default'])
            if length == 0: 
                point_s1[knob] = float(default[index])
                point_s2[knob] = float(default[index])
            else:
                s1_value = label_mapper_s1[s1[knob]]
                point_s1[knob] = length * s1_value + length / 2
                s2_value = label_mapper_s1[s2[knob]]
                point_s2[knob] = length * s2_value + length / 2
                # s2_value = label_mapper_s2[s2[knob]]
                # if s2_value >= len(s2_map[knob]):
                #     point_s2[knob] = detail['max']
                # else: 
                #     point_s2[knob] = s2_map[knob][s2_value]
        best_point = label_data[line['workload']]

        model_config[workload] = point_s2
        repeat = 3
        s1_test = []
        s2_test = []
        best_test = []
        default_test = []
        for j in range(repeat):
            y = stt.test_config(default_point)
            default_test.append(y)
        # for j in range(repeat):
        #     y = stt.test_config(best_point)
        #     best_test.append(y)
        # for j in range(repeat):
        #     y = stt.test_config(point_s1)
        #     s1_test.append(y)

# with open('config.json', 'w') as w:
#     json.dump(model_config, w, indent=4)