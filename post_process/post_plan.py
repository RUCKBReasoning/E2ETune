import json
import copy
import jsonlines

def format_plan(node):
    # 收集当前节点类型
    formatted = f"{node['Node Type'].lower()}"

    # 检查是否有子节点
    if 'Plans' in node:
        child_formats = []
        for child in node['Plans']:
            # 对于每个子节点递归调用该函数
            child_format = format_plan(child)
            child_formats.append(child_format)
        cost = int(node['Total Cost'] - node['Startup Cost'])
        formatted += f"({', '.join(child_formats)}, {cost})"
        print(formatted)

    return formatted

data = []
with open('train5.json') as f:
    # for line in f:
    #     data.append(line)
    data = json.load(f)

all_results = []
for line in data:
    line2 = copy.deepcopy(line)
    all_formatted_plans = []
    for plan in line['plans']:
        print(plan)
        formatted_plans = format_plan(plan['Plan'])
        all_formatted_plans.append(formatted_plans)
    line2['plans'] = all_formatted_plans
    all_results.append(line2)
    
    
with open('train_5.json', 'w') as w:
    json.dump(all_results, w, indent=4)

