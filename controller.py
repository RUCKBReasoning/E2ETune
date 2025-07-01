import csv
import time

from numpy import NaN
import json

import Database
import utils
from config import parse_config
import stress_testing_tool
from tuner import tuner
from knob_config.parse_knob_config import get_knobs

import argparse

# def test_fluctuation(config):
#     knobs_detail = parse_knob_config.get_knobs(config['tuning_config']['knob_config'])
#     knob_default = {}
#     for index, knob in enumerate(knobs_detail):
#         knob_default[knob] = knobs_detail[knob]['default']
#     all_result = []
#     repeat = 5
#     for i in range(repeat):

default = [16.0, 3.0, 200.0, 2048.0, 4096.0, 0.1, 50.0, 600.0, 2.0, -1.0, 0.2, 50.0, 0.0, 2000.0, 64.0, 100.0, 2.0, 0.5, 32.0, 900.0, 0.0, 5.0, 0.1, 1000.0, 100.0, 16384.0, 1.0, 8.0, 5.0, 0.0, 0.0, 0.0, 12.0, 8.0, 16384.0, 128.0, -1.0, 0.0, 200.0, 20.0, 1.0, 10.0, 200.0, 65536.0]


def test_surrogate_result(key, args, config):
    olap = False
    if args['benchmark_config']['workload_path'].startswith('SuperWG'):
        olap = True
        args['benchmark_config']['tool'] = 'dwg'
    else:
        olap = False
        benchmark = args['benchmark_config']['workload_path'].split('/')[0]
        args['benchmark_config']['tool'] = benchmark
        args['benchmark_config']['config_path'] = args['benchmark_config']['workload_path']
        args['database_config']['database'] = 'benchbase'
    database = Database(args, 'knob_config/knob_config.json')
    logger = utils.get_logger(args['tuning_config']['log_path'])
    sample = args['tuning_config']['finetune_sample']
    stt = stress_testing_tool(args, database, logger, sample)
    knobs_detail = get_knobs('knob_config/knob_config.json')
    print(f'test workload {key}')

    cur_point = config
    point = {}
    default_point = {}
    for index, knob in enumerate(knobs_detail):
        # point[knob] = float(cur_point[index])
        default_point[knob] = float(default[index])
    point = config
    repeat = 3
    best_test = []
    default_test = []
    for j in range(repeat):
        y = stt.test_config(default_point)
        default_test.append(y)
    inner = database.fetch_inner_metric()
    for j in range(repeat):
        y = stt.test_config(point)
        best_test.append(y)
    
        # with open(f'all_workload_test{cmd.workload}.txt', 'a') as w:
        #     w.write("step {}: performance: {}\n".format(j, y))
    
    # if max(best_test) > max(default_test):
    if olap:
        with open(f'record/olap_surrogate_record.jsonl', 'a') as w:
            # strs = json.dumps({'workload': key, 'inner': inner, 'default_tps': [float(i) for i in default_test], \
            #             'best_tps': [float(i) for i in best_test], 'best_config': point, \
            #             'delta': (max(best_test) - max(default_test))})
            strs = json.dumps({'workload': key, 'inner': inner, 'best_config': point, 'best': best_test, 'default': default_test})
            w.write(strs + '\n')
    else:
        with open(f'record/oltp_surrogate_record.jsonl', 'a') as w:
            # strs = json.dumps({'workload': key, 'inner': inner, 'default_tps': [float(i) for i in default_test], \
            #             'best_tps': [float(i) for i in best_test], 'best_config': point, \
            #             'delta': (max(best_test) - max(default_test))})
            strs = json.dumps({'workload': key, 'inner': inner, 'best_config': point, 'best': best_test, 'default': default_test})
            w.write(strs + '\n')

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
        