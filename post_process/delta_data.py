import os
import json
import math
import numpy as np
import pandas as pd

setting = 5
mode = 'test'

inner_names = ['cache_hit_rate','concurrent_users','error_rate','logical_reads_per_second','physical_reads_per_second','active_session','transactions_per_second','rows_scanned_per_second','rows_updated_per_second','rows_deleted_per_second']
feature_names = ['size of workload', 'read ratio', 'group by ratio', 'order by ratio', 'aggregation ratio', 'average predicate num per SQL']
knob_details = json.load(open('../knob_config/knob_config.json'))

def format_plan(node):
    # type
    formatted = f"{node['Node Type'].lower()}"
    # check child
    if 'Plans' in node:
        child_formats = []
        for child in node['Plans']:
            child_format = format_plan(child)
            child_formats.append(child_format)
        cost = node['Total Cost'] - node['Startup Cost']
        if cost < 1000:
            cost = format(cost, '.1f')
        elif cost > 1000 and cost < 1000000:
            cost = f'{int(cost/1000)}k'
        elif cost >= 1000000:
            cost = f'{int(cost/1000000)} million'
        formatted += f"({', '.join(child_formats)}, {cost})"

    return formatted

plan_map = {}
plan_data = json.load(open('test5.json'))
for line in plan_data:
    all_formatted_plans = []
    for plan in line['plans']:
        formatted_plans = format_plan(plan['Plan'])
        all_formatted_plans.append(formatted_plans)
    plan_map[line['workload_name']] = all_formatted_plans

data = json.load(open('test6.json'))

results = []
for i, line in enumerate(data):
    inner_metrics = ''
    inner = line['inner_metrics']
    for idx, name in enumerate(inner_names):
        value = inner[idx]
        if value > 0 and value < 1:
            if value < 0.3333: inner_metrics += f'{name}: low; '
            elif value < 0.66667: inner_metrics += f'{name}: middle; '
            else: inner_metrics += f'{name}: high; '
        elif value > 0 and value < 1000:
            inner_metrics += f'{name}: {(int(value * 100)) / 100.0}; '
        elif value < 1000000:
            inner_metrics += f'{name}: {(int(value/ 1000))}k; '
        elif value >= 1000000: inner_metrics += f'{name}: {(int(value/ 100000)) / 10} million; '

    feature = line['feature']
    feature_descrip = ''
    for idx, name in enumerate(feature_names):
        value = feature[idx]
        feature_descrip += f'{name}: {round(value, 2)}; '
    
    # print(inner_metrics)
    workload = line['workload']
    for l in range(2, 20):
        workload = workload.replace(' '*l, ' ')
    if len(workload) > 2500: workload = workload[0:2500]
    if line['workload_name'] not in plan_map.keys():
        continue
    plans = plan_map[line['workload_name']]

    best_config = line['best_config']
    pre_config = line['pre_config']
    output = {}
    input = ''
    if best_config == output:
        continue
    for knob in knob_details:
        detail = knob_details[knob]
        length = detail['max'] - detail['min']
        if length == 0:
            input += f"{knob}: {format(50, '.1f')}%; "
            output[knob] = 'invariant'
            continue
        value1 = (pre_config[knob] - detail['min']) / length
        value2 = (best_config[knob] - detail['min']) / length
        input += f"{knob}: {math.ceil(value1*1000)/10}%; "
        if value2 > value1:
            output[knob] = f"increase {math.ceil((value2 - value1)*1000)/10}%"
        elif value1 == value2: output[knob] = 'invariant'
        else:
            output[knob] = f"reduce {math.ceil((value1 - value2)*1000)/10}%"

    results.append({'database': line['database'], 'workload': line['workload_name'], 'instruction': 'You are an expert in database, you are to optimize the parameters of database, please output in json format, for each field, output one of the change of parameters. The follow information of workloads are offered for you: features, query plans, inner metrics and the current configuration. Every SQL in the workload corresponds to a query plan tree and the query plan tree is represented using parentheses, and each node in the tree carries a cost value estimated by PostgreSQL.', 'input': f"workload features: {feature_descrip} query plans in workload: {'; '.join(plans)}; inner metrics: {inner_metrics}; current config: {input}", 'output': json.dumps(output, ensure_ascii=False)})


with open(f'{mode}_s13.json', 'w') as w:
    json.dump(results, w, indent=4)