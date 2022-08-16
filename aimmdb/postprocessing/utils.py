# import general data science modules
import numpy as np


def simple_moving_average(x, y, window):
    """Reutrn the simple moving average of a data sequence `y` over a rolling window defined on `x`.
    Note that the amount of grid points in an interval always exceeds the amount of spacings of
    that interval by one. The `window` refers to spacings instead of grid points.
    """
    # amount of grid points that accounts for the window in eV (approximately)
    half_window = round(window / 2 / (x[1] - x[0]))

    smoothed = []
    for i in range(len(x)):
        if i <= half_window:  # deal with left bound
            smoothed.append(y[: i + half_window + 1].mean())
        else:  # python handles right bound automatically
            smoothed.append(y[i - half_window : i + half_window + 1].mean())
    return np.array(smoothed)
