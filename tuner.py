import os
import csv
import pickle
import sys
import json
import copy

from pyDOE import lhs

from knob_config import parse_knob_config
import utils
import numpy as np
import pandas as pd
import jsonlines
import random
# tjk add
from Database import Database
from HEBO.hebo.design_space.design_space import DesignSpace
from HEBO.hebo.optimizers.hebo import HEBO
from Vectorlib import VectorLibrary
from stress_testing_tool import stress_testing_tool
from hord_problem import Problem
from poap.controller import BasicWorkerThread, ThreadController
from pySOT.experimental_design import LatinHypercube
from pySOT import strategy, surrogate
from smac.configspace import ConfigurationSpace
from smac.runhistory.runhistory import RunHistory
# from smac.tae.execute_ta_run import Status
from smac.facade.smac_hpo_facade import SMAC4HPO
from smac.scenario.scenario import Scenario
from ConfigSpace.hyperparameters import CategoricalHyperparameter, \
    UniformFloatHyperparameter, UniformIntegerHyperparameter
# from openbox.utils.config_space import ConfigurationSpace, UniformIntegerHyperparameter, \
#     CategoricalHyperparameter, UniformFloatHyperparameter
# from openbox.optimizer.generic_smbo import SMBO


# tjk add
from safe.subspace_adaptation import Safe

tpch_origin = {"max_wal_senders": 21, "autovacuum_max_workers": 126, "max_connections": 860, "wal_buffers": 86880, "shared_buffers": 1114632, "autovacuum_analyze_scale_factor": 78, "autovacuum_analyze_threshold": 1202647040, "autovacuum_naptime": 101527, "autovacuum_vacuum_cost_delay": 45, "autovacuum_vacuum_cost_limit": 1114, "autovacuum_vacuum_scale_factor": 31, "autovacuum_vacuum_threshold": 1280907392, "backend_flush_after": 172, "bgwriter_delay": 5313, "bgwriter_flush_after": 217, "bgwriter_lru_maxpages": 47, "bgwriter_lru_multiplier": 4, "checkpoint_completion_target": 1, "checkpoint_flush_after": 44, "checkpoint_timeout": 758, "commit_delay": 22825, "commit_siblings": 130, "cursor_tuple_fraction": 1, "deadlock_timeout": 885378880, "default_statistics_target": 5304, "effective_cache_size": 1581112576, "effective_io_concurrency": 556, "from_collapse_limit": 407846592, "geqo_effort": 3, "geqo_generations": 1279335040, "geqo_pool_size": 838207872, "geqo_seed": 0, "geqo_threshold": 1336191360, "join_collapse_limit": 1755487872, "maintenance_work_mem": 1634907776, "temp_buffers": 704544576, "temp_file_limit": -1, "vacuum_cost_delay": 46, "vacuum_cost_limit": 5084, "vacuum_cost_page_dirty": 6633, "vacuum_cost_page_hit": 6940, "vacuum_cost_page_miss": 9381, "wal_writer_delay": 4773, "work_mem": 716290752}

def add_noise(knobs_detail, origin_config, range):
    new_config = copy.deepcopy(origin_config)
    for knob in knobs_detail:
        detail = knobs_detail[knob]
        rb = detail['max']
        lb = detail['min']
        if rb - lb <= 1:
            continue
        length = int((rb - lb) * range * 0.5)
        noise = random.randint(-length, length)
        tmp = origin_config[knob] + noise 
        if tmp < lb: tmp = lb
        elif tmp > rb: tmp = rb
        new_config[knob] = tmp
    print(new_config)
    return new_config


class tuner:
    def __init__(self, config):
        if config['benchmark_config']['tool'] != 'surrogate':
            self.database = Database(config, config['tuning_config']['knob_config'])
        else: self.database = None
        self.method = config['tuning_config']['tuning_method']
        self.warmup = config['tuning_config']['warmup_method']
        self.online = config['tuning_config']['online']
        self.online_sample = config['tuning_config']['online_sample']
        self.offline_sample = config['tuning_config']['offline_sample']
        self.finetune_sample = config['tuning_config']['finetune_sample']
        self.inner_metric_sample = config['tuning_config']['inner_metric_sample']
        self.sampling_number = int(config['tuning_config']['sample_num'])
        self.iteration = int(config['tuning_config']['suggest_num'])
        self.knobs_detail = parse_knob_config.get_knobs(config['tuning_config']['knob_config'])
        self.logger = utils.get_logger(config['tuning_config']['log_path'])
        self.ssh_host = config['ssh_config']['host']
        self.last_point = []
        if self.online == 'false':
            self.stt = stress_testing_tool(config, self.database, self.logger, self.offline_sample)
        else:
            self.stt = stress_testing_tool(config, self.database, self.logger, self.finetune_sample)

        # tjk add
        self.pre_safe = None
        self.post_safe = None
        self.veclib = VectorLibrary(config['database_config']['database'])
        features = json.load(open(f"SuperWG/feature/{config['database_config']['database']}.json"))
        self.wl_id = config['benchmark_config']['workload_path']
        if self.warmup == 'workload_map':
            self.feature = features[config['benchmark_config']['workload_path']]
            self.rh_data, self.matched_wl = self.workload_mapper(config['database_config']['database'], 3)
        elif self.warmup == 'rgpe':
            self.feature = features[config['benchmark_config']['workload_path']]
            self.rh_data, self.matched_wl = self.workload_mapper(config['database_config']['database'], 10)

        self.init_safe()

    def workload_mapper(self, database, k):
        matched_wls = self.veclib.find_most_similar(self.feature, k)
        rh_data = []
        keys_to_remove = ["tps", "y", "inner_metrics", "workload"]
        for wl in matched_wls:
            if len(rh_data) > 50:
                break
            if wl == self.wl_id:
                continue
            with jsonlines.open(f'offline_sample/offline_sample_{database}.jsonl') as f:
                for line in f:
                    if line['workload'] == wl:
                        filtered_config = {key: line[key] for key in line if key not in keys_to_remove}
                        rh_data.append({'config': filtered_config, 'tps': line['tps']})
        for wl in matched_wls:
            if wl != self.wl_id:
                best_wl = wl
                break
        return rh_data, best_wl

    # tjk add
    def init_safe(self):
        # 清理上一次调优所临时保存的数据
        if os.path.exists(self.inner_metric_sample):
            with open(self.inner_metric_sample, 'r+') as f:
                f.truncate(0)
        else:
            file = open(self.inner_metric_sample, 'w')
            file.close()
        if os.path.exists(self.offline_sample):
            with open(self.offline_sample, 'r+') as f:
                f.truncate(0)
        else:
            file = open(self.offline_sample, 'w')
            file.close()
        if not os.path.exists(self.offline_sample + '.jsonl'):
            file = open(self.offline_sample + '.jsonl', 'w')
            file.close()

        step = []
        lb, ub = [], []
        knob_default = {}

        for index, knob in enumerate(self.knobs_detail):
            if self.knobs_detail[knob]['type'] == 'integer' or self.knobs_detail[knob]['type'] == 'float':
                lb.append(self.knobs_detail[knob]['min'])
                ub.append(self.knobs_detail[knob]['max'])
            elif self.knobs_detail[knob]['type'] == 'enum':
                lb.append(0)
                ub.append(len(self.knobs_detail[knob]['enum_values']) - 1)
            knob_default[knob] = self.knobs_detail[knob]['default']
            step.append(self.knobs_detail[knob]['step'])

        if self.warmup == 'ours':
            model_config = json.load(open('model_config.json'))
            workload = self.wl_id.split('SuperWG/res/gpt_workloads/')[1]
            knob_default = model_config[workload]
        elif self.warmup == 'pilot':
            origin_config = tpch_origin
            knob_default = add_noise(self.knobs_detail, origin_config, 0.05)

        print('testing default performance...')
        print(knob_default)
        default_performance = self.stt.test_config(knob_default)
        print('default performance: {}'.format(default_performance))
        self.pre_safe = Safe(default_performance, knob_default, default_performance, lb, ub, step)
        with open('safe/predictor.pickle', 'rb') as f:
            self.post_safe = pickle.load(f)
        for i in range(4):
            self.stt.test_config(knob_default)
        self.last_point = list(knob_default.values())
        # 可选择训练后验安全模型
        # self.post_safe.train(data_path='./')

    def tune(self):
        if self.method == 'SMAC':
            self.SMAC()

    def SMAC(self):

        def get_neg_result(point):
            y = self.stt.test_config(point)
            result = -y
            # evaluation_results.append([point, result])
            # print(result)
            return result
        
        cs = ConfigurationSpace()
        print('begin')
        for name in self.knobs_detail.keys():
            detail = self.knobs_detail[name]
            if detail['type'] == 'integer':
                if detail['max'] == detail['min']: detail['max'] += 1
                knob = UniformIntegerHyperparameter(name, detail['min'],\
                                                     detail['max'], default_value=detail['default'])
            elif detail['type'] == 'float':
                knob = UniformFloatHyperparameter(name, detail['min'],\
                                                     detail['max'], default_value=detail['default'])
            cs.add_hyperparameter(knob)

        runhistory = RunHistory()
        if self.warmup == 'workload_map' or self.warmup == 'rgpe':
            for line in self.rh_data:
                continue
                # empty_config = cs.sample_configuration()
                # config = empty_config.import_values(line['config'])
                # config = cs.get_default_configuration().new_configuration(line['config'])
                # runhistory.add(config=config, cost=line['tps'], time=line['tps']*10)
        
        save_workload = self.wl_id.split('SuperWG/res/gpt_workloads/')[1]
        save_workload = save_workload.split('.wg')[0]
        if self.warmup == 'rgpe':
            matched_workload = self.matched_wl.split('SuperWG/res/gpt_workloads/')[1]
            matched_workload = matched_workload.split('.wg')[0]
            scenario = Scenario({"run_obj": "quality",   # {runtime,quality}
                            "runcount-limit": 75,   # max. number of function evaluations; for this example set to a low number
                            "cs": cs,               # configuration space
                            "deterministic": "true",
                            "output_dir": f"./{matched_workload}_smac_output",  
                            "save_model": "true",
                            "local_results_path": f"./models/{save_workload}"
                            })
        else:
            scenario = Scenario({"run_obj": "quality",   # {runtime,quality}
                            "runcount-limit": 75,   # max. number of function evaluations; for this example set to a low number
                            "cs": cs,               # configuration space
                            "deterministic": "true",
                            "output_dir": f"./{save_workload}_smac_output",  
                            "save_model": "true",
                            "local_results_path": f"./models/{save_workload}"
                            })
        
        smac = SMAC4HPO(scenario=scenario, rng=np.random.RandomState(42),tae_runner=get_neg_result, runhistory=runhistory)
        incumbent = smac.optimize()  
        print('finish')
        print(type(incumbent))
        print(incumbent)
        # print(get_neg_result(incumbent))
        runhistory = smac.runhistory
        print(runhistory.data)

        def runhistory_to_json(runhistory):
            data_to_save = {}
            for run_key in runhistory.data.keys():
                config_id, instance_id, seed, budget = run_key
                run_value = runhistory.data[run_key]
                data_to_save[str(run_key)] = {
                    "cost": run_value.cost,
                    "time": run_value.time,
                    "status": run_value.status.name,
                    "additional_info": run_value.additional_info
                }
            return json.dumps(data_to_save, indent=4)

        with open(f"smac_his/{save_workload}_{self.warmup}.json", "w") as f:
            f.write(runhistory_to_json(runhistory))

    # def RGPE(self):
    #     matched_workload = self.matched_wl.split('SuperWG/res/gpt_workloads/')[1]
    #     matched_workload = matched_workload.split('.wg')[0]
    #     return 