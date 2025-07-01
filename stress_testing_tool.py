import copy
import os
import sys
import time
import numpy as np
from tuning_utils.new_task2 import *
from tuning_utils.surrogate import *
from knob_config import parse_knob_config


class stress_testing_tool:
    def __init__(self, config, db, logger, records_log):
        self.benchmark_config = config['benchmark_config']
        self.db = db
        self.ssh_config = config['ssh_config']
        self.sur_config = config['surrogate_config']
        self.logger = logger
        self.records_log = records_log

    def test_config(self, config):
        temp_config = copy.deepcopy(config)

        cur_state = []
        if self.benchmark_config['tool'] != 'surrogate':
            flag = self.db.change_knob(config)
            cur_state = self.db.fetch_inner_metric()

        log_file = sys.path[0] + '/log/{}.log'.format(int(time.time()))

        if self.benchmark_config['tool'] == 'sysbench':
            y = self.test_by_sysbench()
        elif self.benchmark_config['tool'] == 'tpcc':
            y = self.test_by_tpcc(log_file)
        elif self.benchmark_config['tool'] == 'dwg':
            y = self.test_by_dwg(self.benchmark_config['workload_path'], self.benchmark_config['log_path'])
            y = y[0]
        elif self.benchmark_config['tool'] == 'surrogate':
            y = self.test_by_surrogate(cur_state, self.benchmark_config['workload_path'], self.sur_config, config)

        f = open(self.records_log, 'a')
        temp_config['tps'] = y
        f.writelines(json.dumps(temp_config) + '\n')
        f.close()
        
        if self.benchmark_config['tool'] != 'surrogate':
            f = open('offline_sample/offline_sample.jsonl', 'a')
            temp_config['y'] = [-y, 1/(-y)]
            temp_config['inner_metrics'] = cur_state
            temp_config['workload'] = self.benchmark_config['workload_path']
            f.writelines(json.dumps(temp_config) + '\n')
            f.close()

        workload = self.benchmark_config['workload_path'].split('/')[-1]
        with open(f'performance_record/{workload}.txt', 'a') as w:
            w.write("performance: {}\n".format(-y))

        return y

    # tjk modified
    def test_by_sysbench(self):
        command = 'sysbench --db-driver=pgsql --time={} --threads=100 --report-interval=1 --pgsql-host={} ' \
                  '--pgsql-port={} --pgsql-user={} --pgsql-password={} --pgsql-db={} ' \
                  '--tables={} --table_size={} {} --db-ps-mode=disable run'.format(
            int(self.benchmark_config['time']) + 2,
            self.db.host,
            self.db.port,
            self.db.user,
            self.db.password,
            self.db.database,
            self.benchmark_config['tables'],
            self.benchmark_config['table_size'],
            self.benchmark_config['mode']
        )
        # tjk add
        results = []
        # _, out, _ = self.db.ssh.exec_command(command)

        out = os.popen(command)

        lines = out.readlines()
        lines = lines[21:int(self.benchmark_config['time']) + 11]
        for line in lines:
            if line.find("tps") == -1:
                continue
            content = line.split(" ")
            results.append(float(content[6]))

        return np.array(results).mean()

    def test_by_tpcc(self, log_file):
        command = '/usr/local/benchmarksql-5.0/run/runBenchmark.sh props.pg > {}'.format(log_file)
        state = os.system(command)

        if state == 0:
            self.logger.info('tpcc running success')
        else:
            self.logger.info('tpcc running error')

        with open(log_file) as f:
            lines = f.readlines()
        for line in lines:
            if 'Measured tpmTOTAL' in line:
                tps = float(line.split()[9])

        return tps

    def test_by_dwg(self, workload_path, log_file):
        mh = multi_thread(self.db, workload_path, int(self.benchmark_config['thread']), log_file)

        mh.data_pre()
        return mh.run()
    
    def test_by_surrogate(self, inner_metrics, workload_path, sur_config, knobs):
        sg = Surrogate(sur_config, workload_path)
        knob_detail = parse_knob_config.get_knobs('knob_config/knob_config.json')
        x = []
        for i, key in enumerate(knob_detail.keys()):
            detail = knob_detail[key]
            if detail['max'] - detail['min'] != 0:
                x.append((knobs[key] - detail['min']) / (detail['max'] - detail['min']))
            else: continue

        return sg.run(inner_metrics, x)