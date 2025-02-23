import os
import json
import random


length_mapper = {
    'tpcc': 5,
    'smallbank': 6,
    'twitter': 5,
    'wikipedia': 5,
    'ycsb': 6
}

standards_mapper = {
    'tpcc': [45,43,4,4,4],
    'smallbank': [15,15,15,25,15,15],
    'twitter': [1,1,7,90,1],
    'wikipedia': [1,1,7,90,1],
    'ycsb': [50,5,15,10,10,10]
}

def generate(workload):
    with open(f'{workload}/sample_{workload}_config0.xml') as f:
        file = f.read()

    head = file.split('<weights>')[0]
    tail = file.split('</weights>')[1]

    length = length_mapper[workload]
    standard = standards_mapper[workload]

    all_results = json.load(open(f'{workload}/mapper.json'))
    for k in range(100):
        unfinish = True
        while(unfinish):
            result = []
            residue = 100
            for i in range(length-1):
                if residue == 0:
                    unfinish = True
                    break
                try:
                    new_v = random.randint(0, residue)
                    result.append(new_v)
                    residue -= new_v
                except:
                    unfinish = True
                    break
            result.append(residue)
            print(sum(result))
            if len(result) == length:
                random.shuffle(result)
                if result != standard:
                    unfinish = False

            if not os.path.exists(f'{workload}/'):
                os.mkdir(f'{workload}/')

            with open(f'{workload}/sample_{workload}_config{k+100}.xml', 'w') as f:
                result_ = [str(i) for i in result]
                f.write(head + '<weights>' + ','.join(result_) + '</weights>' + tail)

            all_results[f'{workload}/sample_{workload}_config{k+100}.xml'] = result


    with open(f'{workload}/mapper.json', 'w') as f:
        json.dump(all_results,f, indent=4)



# for workload in length_mapper.keys():
#     print(workload)
    # generate(workload)
generate('ycsb')