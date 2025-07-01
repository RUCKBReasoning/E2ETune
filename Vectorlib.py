import json
import numpy as np
from scipy.spatial import distance

class VectorLibrary:
    def __init__(self, database):
        # if database == 'tpch24':
        #     database = 'tpch'
        with open(f'SuperWG/feature/{database}.json') as f:
            self.vector_map = json.load(f)
        # self.vectors = [] 
        # self.ids = [] 

    def add_vector(self, vector, vector_id):
        self.vector_map[vector_id] = vector

    def find_most_similar(self, new_vector, n=1):
        similarities = []
        ids = list(self.vector_map.keys())
        vectors = self.vector_map.values()
        for id in ids:
            vector = self.vector_map[id]
            sim = distance.euclidean(vector, new_vector)
            similarities.append(sim)

        most_similar_indices = np.argsort(similarities)[:n]
        return [ids[i] for i in most_similar_indices]
