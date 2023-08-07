import uuid
from collections.abc import Iterable
from pprint import pprint
import numpy as np
from preference import UserProperties


def new_user():
    id = str(np.random.randint(1, 1000_000_000))

    activeness = np.exp(np.random.lognormal(mean=1))
    # routines = generate_routine_dict(activeness)
    distrib = dict(userid=id, activeness=activeness)

    for tag, gen in UserProperties.generators.items():
        distrib[tag] = float(gen())
    return distrib


if __name__ == "__main__":
    pprint(new_user(), indent=2)
