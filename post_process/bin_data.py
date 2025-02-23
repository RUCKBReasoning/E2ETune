import os
import json
import math
import numpy as np
import pandas as pd

setting = 5
mode = 'train'

label_mapper_s0 = {
    0: 'very low',
    1: 'low', 
    2: 'middle',
    3: 'high',
    4: 'very high'
}

label_mapper_s1 = {
    0: '00% to 10%',
    1: '10% to 20%',
    2: '20% to 30%',
    3: '30% to 40%',
    4: '40% to 50%',
    5: '50% to 60%',
    6: '60% to 70%',
    7: '70% to 80%',
    8: '80% to 90%',
    9: '90% to 100%'
}

label_mapper_s2 = {
    0: 'extremely low',
    1: 'very low',
    2: 'low', 
    3: 'slightly low',
    4: 'middle',
    5: 'slightly high',
    6: 'high',
    7: 'very high',
    8: 'extremyly high'
}

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
        print(formatted)

    return formatted

file = f'{mode}5.json'
with open(file) as f:
    data = json.load(f)

inner_names = ['cache_hit_rate','concurrent_users','error_rate','logical_reads_per_second','physical_reads_per_second','active_session','transactions_per_second','rows_scanned_per_second','rows_updated_per_second','rows_deleted_per_second']
feature_names = ['size of workload', 'read ratio', 'group by ratio', 'order by ratio', 'aggregation ratio', 'average predicate num per SQL']
knob_details = json.load(open('../knob_config/knob_config.json'))

bin_best_labels = {}
bin_best_edges = {}
bin_map_s2 = {}
if setting % 3 == 2:
    best_configs = {}
    for knob in knob_details:
        best_configs[knob] = []
    for line in data:
        best = line['best_config']
        for knob in knob_details:
            best_configs[knob].append(best[knob])
    labels = list(label_mapper_s2.values())
    for knob in knob_details:
        if knob == 'temp_file_limit': continue
        buckets = pd.qcut(best_configs[knob], 9, labels=False, retbins=True, duplicates='drop')
        bin_best_labels[knob] = buckets[0]
        bin_best_edges[knob] = buckets[1]

    for knob in bin_best_edges.keys():
        edges = bin_best_edges[knob]
        values = []
        for i in range(len(edges)-1):
            values.append((edges[i] + edges[i+1]) / 2)
        bin_map_s2[knob] = values

    with open('map_s2.json', 'w') as w:
        json.dump(bin_map_s2, w, indent=4)


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
    if 'plans' in line.keys():
        all_formatted_plans = []
        for plan in line['plans']:
            formatted_plans = format_plan(plan['Plan'])
            all_formatted_plans.append(formatted_plans)

    best_config = line['best_config']
    output = {}
    for knob in knob_details:
        detail = knob_details[knob]
        value = best_config[knob]
        if setting % 3 == 1:
            length = (detail['max'] - detail['min']) / 10
            if length == 0: output[knob] = 'middle'
            else:
                delta = math.floor((value - detail['min']) / length)
                if delta == 10: delta -= 1
                output[knob] = label_mapper_s1[delta]
        elif setting % 3 == 2:
            if knob == 'temp_file_limit': output[knob] = 'middle'
            else: 
                output[knob] = label_mapper_s2[bin_best_labels[knob][i]]
        elif setting % 3 == 0:
            default = detail['default']
            if default == 0 or default == -1:
                output[knob] = value
            else:
                delta = value - default
                if delta < 0:
                    output[knob] = f'reduce {math.ceil(-delta / default * 100)}%'
                elif delta == 0: output[knob] = 'invariant'
                else:
                    if delta / default > 1:
                        if delta / default > 1000:
                            output[knob] = f'increase {math.ceil(delta / (default * 1000))}k fold'
                        else: output[knob] = f'increase {math.ceil(delta / default)} fold'
                    else:
                        output[knob] = f'increase {math.ceil(delta / default * 100)}%'

    if setting == 1:
        results.append({'database': line['database'], 'workload': line['workload_name'], 'instruction': 'You are an expert in database, you are to optimize the parameters of database, please output in json format, for each field, output one of "00% to 10%", "10% to 20%", "20% to 30%", "40% to 50%", "50% to 60%", "60% to 70%", "70% to 80%", "80% to 90%", "90% to 100%"', 'input': f'workload features: {feature_descrip} workload: {workload}; inner metrics: {inner_metrics}', 'output': json.dumps(output, ensure_ascii=False)})
    elif setting == 2:
        results.append({'database': line['database'], 'workload': line['workload_name'], 'instruction': 'You are an expert in database, you are to optimize the parameters of database, please output in json format, for each field, output one of "extremely low", "very low", "low", "slightly low", "middle", "slightly high", "high", "very high", "extremely high"', 'input': f'workload features: {feature_descrip} workload: {workload}; inner metrics: {inner_metrics}', 'output': json.dumps(output, ensure_ascii=False)})
    elif setting == 3:
        results.append({'database': line['database'], 'workload': line['workload_name'], 'instruction': 'You are an expert in database, you are to optimize the parameters of database, please output in json format, for each field, output one of the change of parameters.', 'input': f'workload features: {feature_descrip} workload: {workload}; inner metrics: {inner_metrics}', 'output': json.dumps(output, ensure_ascii=False)})
    elif setting == 4:
        results.append({'database': line['database'], 'workload': line['workload_name'], 'instruction': 'You are an expert in database, you are to optimize the parameters of database, please output in json format, for each field, output one of "00% to 10%", "10% to 20%", "20% to 30%", "40% to 50%", "50% to 60%", "60% to 70%", "70% to 80%", "80% to 90%", "90% to 100%". The follow information of workloads are offered for you: features, query plans and inner metrics. Every SQL in the workload corresponds to a query plan tree and the query plan tree is represented using parentheses, where each node is followed by a pair of parentheses containing its child nodes, with sub-nodes separated by parentheses, recursively showing the entire tree\'s hierarchical structure. Additionally, each node carries a cost value estimated by PostgreSQL.', 'input': f"workload features: {feature_descrip} query plans in workload: {'; '.join(all_formatted_plans)}; inner metrics: {inner_metrics}", 'output': json.dumps(output, ensure_ascii=False)})
    elif setting == 5:
        results.append({'database': line['database'], 'workload': line['workload_name'], 'instruction': 'You are an expert in database, you are to optimize the parameters of database, please output in json format, for each field, output one of "extremely low", "very low", "low", "slightly low", "middle", "slightly high", "high", "very high", "extremely high". The follow information of workloads are offered for you: features, query plans and inner metrics. Every SQL in the workload corresponds to a query plan tree and the query plan tree is represented using parentheses, where each node is followed by a pair of parentheses containing its child nodes, with sub-nodes separated by parentheses, recursively showing the entire tree\'s hierarchical structure. Additionally, each node carries a cost value estimated by PostgreSQL.', 'input': f"workload features: {feature_descrip} query plans in workload: {'; '.join(all_formatted_plans)}; inner metrics: {inner_metrics}", 'output': json.dumps(output, ensure_ascii=False)})
    elif setting == 6:
        results.append({'database': line['database'], 'workload': line['workload_name'], 'instruction': 'You are an expert in database, you are to optimize the parameters of database, please output in json format, for each field, output one of the change of parameters. The follow information of workloads are offered for you: features, query plans and inner metrics. Every SQL in the workload corresponds to a query plan tree and the query plan tree is represented using parentheses, where each node is followed by a pair of parentheses containing its child nodes, with sub-nodes separated by parentheses, recursively showing the entire tree\'s hierarchical structure. Additionally, each node carries a cost value estimated by PostgreSQL.', 'input': f"workload features: {feature_descrip} query plans in workload: {'; '.join(all_formatted_plans)}; inner metrics: {inner_metrics}", 'output': json.dumps(output, ensure_ascii=False)})
    
with open(f'{mode}_s{setting+6}.json', 'w') as w:
    json.dump(results, w, indent=4)