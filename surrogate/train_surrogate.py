# {"max_wal_senders": 18, "autovacuum_max_workers": 5, "max_connections": 198, "wal_buffers": 2048, "shared_buffers": 8194, "autovacuum_analyze_scale_factor": 0, "autovacuum_analyze_threshold": 34, "autovacuum_naptime": 730, "autovacuum_vacuum_cost_delay": 19, "autovacuum_vacuum_cost_limit": 1, "autovacuum_vacuum_scale_factor": 0, "autovacuum_vacuum_threshold": 127, "backend_flush_after": 0, "bgwriter_delay": 2017, "bgwriter_flush_after": 62, "bgwriter_lru_maxpages": 85, "bgwriter_lru_multiplier": 3, "checkpoint_completion_target": 0, "checkpoint_flush_after": 32, "checkpoint_timeout": 926, "commit_delay": 3, "commit_siblings": 4, "cursor_tuple_fraction": 0, "deadlock_timeout": 974, "default_statistics_target": 117, "effective_cache_size": 16243, "effective_io_concurrency": 1, "from_collapse_limit": 14, "geqo_effort": 7, "geqo_generations": 8, "geqo_pool_size": 7, "geqo_seed": 0, "geqo_threshold": 33, "join_collapse_limit": 23, "maintenance_work_mem": 16094, "temp_buffers": 113, "temp_file_limit": -1, "vacuum_cost_delay": 0, "vacuum_cost_limit": 226, "vacuum_cost_page_dirty": 5, "vacuum_cost_page_hit": 0, "vacuum_cost_page_miss": 3, "wal_writer_delay": 209, "work_mem": 65478, "tps": -0.7729678452014923, "y": [0.7729678452014923, 1.2937148759911563], "inner_metrics": [0.005641747939026364, 1.0, 0.01773049645390071, 3745074.722383502, 74752.4833680874, 6.0, 4.933528363843051, 3744791.0890290104, 0.0, 0.0], "workload": "SuperWG/res/gpt_workloads/5.wg"}
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, VotingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import cross_val_score, train_test_split, KFold
from sklearn.metrics import r2_score
import json
import jsonlines
import joblib
import random
import numpy as np


def my_cross_val(model, data, features, database):
    scores = []
    k = 0
    best = 0
    while k < 10:
        X_train = []; X_test = []; y_train = []; y_test = []
        test = random.sample(data.keys(), 3)

        for key in data.keys():
            tmp = data[key]
            if len(tmp) <= 10 or key not in features.keys():
                continue
            feature = features[key]
            l = max([i[1] for i in tmp]) 
            r = min([i[1] for i in tmp])
            if key not in test:
                X_train += [i[0] + feature for i in tmp]
                y_train += [((i[1] - r) / (l - r)) for i in tmp]
            else:
                X_test += [i[0] + feature for i in tmp]
                y_test += [((i[1] - r) / (l - r)) for i in tmp]
        try: 
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            score = r2_score(y_true=y_test, y_pred=y_pred)
        except: score = 0
        if score > 0:
            scores.append(score)
            print(f"Fold {k} R^2 Score: {score:.4f}")
            k += 1
            if score > best:
                best = score
                model_filename = f'surrogate/{database}.pkl'
                joblib.dump(model, model_filename)


    mean_score = np.mean(scores)
    print(f"Mean R^2 Score: {mean_score:.4f}")

    return scores


def train_surrogate(database):
    print('training surrogate model...')
    knobs = json.load(open('knob_config/knob_config.json'))
    features = json.load(open(f'SuperWG/feature/{database}.json'))

    data = {}
    with jsonlines.open(f'offline_sample/offline_sample_{database}.jsonl', 'r') as f:
        for record in f:
            x = []
            # y.append(-record['tps'])
            for key in record.keys():
                if key == 'y' or key == 'workload' or key == 'tps' or key == 'inner_metrics': continue
                else:
                    detail = knobs[key]
                    if detail['max'] - detail['min'] != 0:
                        x.append((record[key] - detail['min']) / (detail['max'] - detail['min']))
                    else: continue
                    # x.append(record[key])
            # x += record['inner_metrics']
            if record['workload'] in data.keys(): data[record['workload']].append([x, record['tps']])
            else: data[record['workload']] = [[x, record['tps']]]
            # print(x)
            # X.append(x)

    rf = RandomForestRegressor(n_estimators=500, random_state=42)
    gb = GradientBoostingRegressor(random_state=42)
    reg = VotingRegressor(estimators=[('gb', rf), ('rf', gb)])

    # model_filename = 'random_forest_model4.pkl'
    # joblib.dump(reg, model_filename)
    # loaded_model = joblib.load(model_filename)

    # kf = KFold(n_splits=10, shuffle=True, random_state=42)

    my_cross_val(reg, data, features, database)
