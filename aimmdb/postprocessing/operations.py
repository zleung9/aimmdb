"""Module for housing post-processing operations."""

from abc import ABC, abstractmethod
from datetime import datetime
from uuid import uuid4

from monty.json import MSONable
import numpy as np
import pandas as pd
from scipy.interpolate import InterpolatedUnivariateSpline
from sklearn.linear_model import LinearRegression

from aimm_post_processing import utils
from tiled.client.dataframe import DataFrameClient
from tiled.client.node import Node


class Operator(MSONable, ABC):
    """Base operator class. Tracks everything required through a combination
    of the MSONable base class and by using an additional datetime key to track
    when the operator was logged into the metadata of a new node.

    .. important::

        The __call__ method must be derived for every operator. In particular,
        this operator should take as arguments at least one data point (node).
    """

    @abstractmethod
    def _process_data(self):
        ...

    @abstractmethod
    def _process_metadata(self):
        ...

    @abstractmethod
    def __call__(self, client):
        ...


class UnaryOperator(Operator):
    """Specialized operator class which takes only a single input. This input
    must be of instance :class:`DataFrameClient`.

    Particularly, the operator object's ``__call__`` method can be executed on
    either a :class:`DataFrameClient` or :class:`Node` object. If run on the
    :class:`DataFrameClient`, the operator call will return a single dictionary
    with the keys "data" and "metadata", as one would expect. If the input is
    of type :class:`Node`, then an attempt is made to iterate through all
    children of that node, executing the operator on each instance
    individually. A list of dictionaries is then returned.
    """

    def _process_metadata(self, metadata):
        """Processing of the metadata dictionary object. Takes the
        :class:`dict` object as input and returns a modified
        dictionary with the following changes:

            1. metadata["_tiled"]["uid"] is replaced with a new uuid string.
            2. metadata["post_processing"] is created with keys that indicate
               the current state of the class, the parent ids

        Parameters
        ----------
        metadata : dict
            The metadata dictionary accessed via ``df_client.metadata``.

        Returns
        -------
        dict
            The new metadata object for the post-processed child.
        """

        dt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        return {
            "_tiled": {"uid": str(uuid4())},  # Assign a new uid
            "post_processing": {
                "parents": [metadata["_tiled"]["uid"]],
                "operator": self.as_dict(),
                "kwargs": self.__dict__,
                "datetime": f"{dt} UTC",
            },
        }

    def _call_on_client(self, client):
        return {
            "data": self._process_data(client.read()),
            "metadata": self._process_metadata(client.metadata),
        }

    def __call__(self, client):
        if isinstance(client, DataFrameClient):
            return self._call_on_client(client)

        elif isinstance(client, Node):
            # Apply the operator to each of the instances in the node
            values = [value for value in client.values()]
            if not all([isinstance(v, DataFrameClient) for v in values]):
                raise RuntimeError(
                    "Provided client when iterated on has produced entries "
                    "that are not DataFrameClient objects. This is likely "
                    "due to passing a query such as "
                    'df_client = c["edge"]["K"], when a query like '
                    'df_client = c["edge"]["K"]["uid"] is required'
                )
            return list(map(self._call_on_client, values))

        else:
            raise ValueError(
                f"client is of type {type(client)} but should be of "
                "type DataFrameClient"
            )


class Identity(UnaryOperator):
    """The identity operation. Does nothing. Primarily used for testing
    purposes."""

    def __init__(self):
        super().__init__()

    def _process_data(self, df):
        """
        Parameters
        ----------
        df : pandas.DataFrame
            The dataframe object to process.

        Returns
        -------
        pandas.DataFrame
        """

        return df


class StandardizeGrid(UnaryOperator):
    """Interpolates specified columns onto a common grid.

    Parameters
    ----------
    x0 : float
        The lower bound of the grid to interpolate onto.
    xf : float
        The upper bound of the grid to interpolate onto.
    nx : int
        The number of interpolation points.
    interpolated_univariate_spline_kwargs : dict, optional
        Keyword arguments to be passed to the
        :class:`InterpolatedUnivariateSpline`. See
        [here](https://docs.scipy.org/doc/scipy/reference/generated/
        scipy.interpolate.InterpolatedUnivariateSpline.html) for the
        documentation on this class.
    x_column : str, optional
        References a single column in the DataFrameClient (this is the
        "x-axis").
    y_columns : list, optional
        References a list of columns in the DataFrameClient (these are the
        "y-axes").
    """

    def __init__(
        self,
        *,
        x0,
        xf,
        nx,
        interpolated_univariate_spline_kwargs=dict(),
        x_column="energy",
        y_columns=["mu"],
    ):
        self.x0 = x0
        self.xf = xf
        self.nx = nx
        self.interpolated_univariate_spline_kwargs = (
            interpolated_univariate_spline_kwargs
        )
        self.x_column = x_column
        self.y_columns = y_columns

    def _process_data(self, df):
        """Takes in a dictionary of the data amd metadata. The data is a
        :class:`pd.DataFrame`, and the metadata is itself a dictionary.
        Returns the same dictionary with processed data and metadata.
        """

        new_grid = np.linspace(self.x0, self.xf, self.nx)
        new_data = {self.x_column: new_grid}
        for column in self.y_columns:
            ius = InterpolatedUnivariateSpline(
                df[self.x_column],
                df[column],
                **self.interpolated_univariate_spline_kwargs,
            )
            new_data[column] = ius(new_grid)

        return pd.DataFrame(new_data)


class RemoveBackground(UnaryOperator):
    """Fit the pre-edge region to a Victoreen function and subtract it from the
    spectrum.

    Parameters
    ----------
    x0 : float
        The lower bound of energy range on which the background is fitted.
    xf : float
        The upper bound of energy range on which the background is fitted.
    x_column : str, optional
        References a single column in the DataFrameClient (this is the
        "x-axis").
    y_columns : list, optional
        References a list of columns in the DataFrameClient (these are the
        "y-axes").
    victoreen_order : int
        The order of Victoreen function. The selected data is fitted to
        Victoreen pre-edge  function (in which one fits a line to
        :math:`E^n \\mu(E)` for some value of :math:`n`.
    """

    def __init__(
        self, *, x0, xf, x_column="energy", y_columns=["mu"], victoreen_order=0
    ):
        self.x0 = x0
        self.xf = xf
        self.victoreen_order = victoreen_order
        self.x_column = x_column
        self.y_columns = y_columns

    def _process_data(self, df):
        """
        Notes
        -----
        ``LinearRegression().fit()`` takes 2-D arrays as input. This can be
        explored for batch processing of multiple spectra
        """

        bg_data = df.loc[
            (df[self.x_column] >= self.x0) * (df[self.x_column] < self.xf)
        ]

        new_data = {self.x_column: df[self.x_column]}
        for column in self.y_columns:
            y = bg_data[column] * bg_data[self.x_column] ** self.victoreen_order
            reg = LinearRegression().fit(
                bg_data[self.x_column].to_numpy().reshape(-1, 1),
                y.to_numpy().reshape(-1, 1),
            )
            background = reg.predict(
                df[self.x_column].to_numpy().reshape(-1, 1)
            )
            new_data[column] = (
                df.loc[:, column].to_numpy() - background.flatten()
            )

        return pd.DataFrame(new_data)


class StandardizeIntensity(UnaryOperator):
    """Scale the intensity so they vary in similar range. Specifically, aligns
    the intensity to the mean of a selected range, and scale the intensity up
    to standard deviation.

    Parameters
    ----------
    x0 : float
        The lower bound of energy range for which the mean is calculated. If None, the first
        point in the energy grid is used. Default is None.
    yf : float
        The upper bound of energy range for which the mean is calculated. If None, the last
        point in the energy grid is used. Default is None.
    x_column : str, optional
            References a single column in the DataFrameClient (this is the
            "x-axis"). Default is "energy".
    y_columns : list, optional
        References a list of columns in the DataFrameClient (these are the
        "y-axes"). Default is ["mu"].
    """

    def __init__(self, *, x0, xf, x_column="energy", y_columns=["mu"]):
        self.x0 = x0
        self.xf = xf
        self.x_column = x_column
        self.y_columns = y_columns

    def _process_data(self, df):
        """
        Takes in a dictionary of the data amd metadata. The data is a
        :class:`pd.DataFrame`, and the metadata is itself a dictionary.
        Returns the same dictionary with processed data and metadata.

        """

        grid = df.loc[:, self.x_column]
        if self.x0 is None:
            self.x0 = grid[0]
        if self.xf is None:
            self.xf = grid[-1]
        assert self.x0 < self.xf, "Invalid range, make sure x0 < xf"
        select_mean_range = (grid > self.x0) & (grid < self.xf)

        new_data = {self.x_column: df[self.x_column]}
        for column in self.y_columns:
            mu = df.loc[:, column]
            mu_mean = mu[select_mean_range].mean()
            mu_std = mu.std()
            new_data.update({column: (mu - mu_mean) / mu_std})

        return pd.DataFrame(new_data)


class Smooth(UnaryOperator):
    """Return the simple moving average of spectra with a rolling window.

    Parameters
    ----------
    window : float, in eV.
        The rolling window in eV over which the average intensity is taken.
    x_column : str, optional
        References a single column in the DataFrameClient (this is the
        "x-axis").
    y_columns : list, optional
        References a list of columns in the DataFrameClient (these are the
        "y-axes").
    """

    def __init__(self, *, window=10.0, x_column="energy", y_columns=["mu"]):
        self.window = window
        self.x_column = x_column
        self.y_columns = y_columns

    def _process_data(self, df):
        """
        Takes in a dictionary of the data amd metadata. The data is a
        :class:`pd.DataFrame`, and the metadata is itself a dictionary.
        Returns the same dictionary with processed data and metadata.

        Returns:
        --------
        dict
            A dictionary of the data and metadata. The data is a :class:`pd.DataFrame`,
            and the metadata is itself a dictionary.
        """

        grid = df.loc[:, self.x_column]
        new_data = {self.x_column: df[self.x_column]}
        for column in self.y_columns:
            y = df.loc[:, column]
            y_smooth = utils.simple_moving_average(grid, y, window=self.window)
            new_data.update({column: y_smooth})

        return pd.DataFrame(new_data)


# TODO
class Classify(UnaryOperator):
    """Label the spectrum as "good", "noisy" or "discard" based on the quality
    of the spectrum."""

    ...


# TODO
class PreNormalize(UnaryOperator):
    ...
