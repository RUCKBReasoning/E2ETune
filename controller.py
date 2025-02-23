import csv
import time

from numpy import NaN
import json

import Database
import utils
from config import parse_config
from knob_ranking.shap_final import knob_selection
from knowledge_transfer.mapping import mapping, get_best_config
from knowledge_transfer.update_knowledge import update_knowledge
import stress_testing_tool
from tuner import tuner
from knowledge_transfer.get_feature import get_feature
from knob_config import parse_knob_config
from workload_select import test_surrogate_result

import argparse

# def test_fluctuation(config):
#     knobs_detail = parse_knob_config.get_knobs(config['tuning_config']['knob_config'])
#     knob_default = {}
#     for index, knob in enumerate(knobs_detail):
#         knob_default[knob] = knobs_detail[knob]['default']
#     all_result = []
#     repeat = 5
#     for i in range(repeat):

def tune(workload, host, args):
    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # try: 
    t = tuner(args).tune()
    # except Exception as e:
    #     print(f'an error occurred during tuning: {e}')
    
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def get_tps(line):
        if line.find("tps") == -1:
            return 'nan'
        tps = line[line.find("tps"):]
        tps = tps.split(":")[1]
        tps = tps.split("}")[0].strip()
        config = line[1:line.find('tps') - 3]
        if tps == "NaN":
            return 'nan'
        return float(tps), config
    
    with open(args['tuning_config']['offline_sample'], 'r') as f:
        lines = f.readlines()
        default_performance = lines[0][lines[0].find("tps") + 6:]
        default_performance = default_performance.split("}")[0]
        all_default = []
        best_tps = float(default_performance)
        best_config = ''
        for i in range(5):
            tps, _ = get_tps(lines[i])
            all_default.append(tps)
        for line in lines:
            tps, config = get_tps(line)
            if best_tps < tps:
                best_tps = tps
                best_config = config
    if args['benchmark_config']['tool'] != 'surrogate':
        delta = best_tps - max(all_default)
        print(all_default, delta)
        
        inner = json.load(open(f'record/inner_metrics{host}.json'))['inner']
        with open(f'record/offine_record.jsonl', 'a') as w:
            strs = json.dumps({'workload': workload, 'inner': inner, 'default_tps': [float(i) for i in all_default], \
                        'best_tps': best_tps, 'best_config': best_config, 'undulation': max(all_default) - min(all_default), \
                        'delta': delta})
            w.write(strs + '\n')
    else:
        try: 
            best_config = '{' + best_config + '}'
            print(best_config)
            best_config = json.loads(best_config.strip())
            test_surrogate_result(workload, args=args, config=best_config)
        except:
            with open(f'record/offine_record.jsonl', 'a') as w:
                strs = json.dumps({'workload': workload, 'default_tps': float(default_performance), \
                            'best_tps': best_tps, 'best_config': best_config})
                w.write(strs + '\n')
        