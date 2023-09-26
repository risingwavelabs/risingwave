from functools import partial, reduce
import numpy as np


def _chain(*funcs):
    def inner():
        res = None
        for f in funcs:
            if not res:
                res = f()
            else:
                res = f(res)
        return res
    return inner



class UserProperties:
    generators = {
        "activeness": _chain(
            partial(np.random.lognormal, mean=10, sigma=5),
            partial(np.clip, a_min=1, a_max=50)
        ),
        "address_lat": partial(np.random.uniform, low=-180, high=180),
        "address_long": partial(np.random.uniform, low=-180, high=180),
        "age_approx": partial(np.random.randint, low=18, high=100),
        "gender": partial(np.random.choice, [0, 1]),
        "occupation": partial(np.random.randint, low=0, high=25),
    }
