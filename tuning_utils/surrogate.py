import joblib
import json
import os


class Surrogate:
    def __init__(self, sur_config, workload_path) -> None:
        self.model_name = sur_config['model_name']
        self.model = joblib.load(sur_config['model_path'])
        self.workload = workload_path
        self.workload_feature = self.read_feature(sur_config['feature_path'])
    
    def read_feature(self, path):
        if os.path.exists(path):
            with open(path) as f:
                features = json.load(f)
            return features[self.workload]
        else: return []

    def run(self, inner_metrics, config):
        x = config + inner_metrics + self.workload_feature
        print(f'performance: {self.model.predict([x])[0] * 10}')
        return self.model.predict([x])[0] * 10