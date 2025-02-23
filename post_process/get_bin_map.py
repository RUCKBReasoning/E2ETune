import os
import json
import math
import numpy as np
import pandas as pd

setting = 3
mode = 'test'

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

