from monty.json import MSONable
import tiled
from aimm_post_processing import operations as op



class Pipeline(MSONable):
    """The data pipeline that contains a chain of operations
    """
    def __init__(self, operation_list):
        self.pull_first = False # whether has Pull as the first operation
        if isinstance(operation_list[0], op.Pull):
            self.pull_first = True
            self.init_operation = operation_list.pop(0)
        else:
            self.init_operation = op.Identity()
        self.operation_list = operation_list

    def run(self, data):
        if self.pull_first:
            assert isinstance(data, tiled.client.dataframe.DataFrameClient), \
                """The first operation of your pipeline is Pull, which only takes
                "tiled.client.dataframe.DataFrameClient" as input.
                """
        else:
            assert isinstance(data, dict), \
                """No Pull operation found, only python dictionary is accepted.
                """
        data = self.init_operation(data)
        pp_history = {0: data["metadata"]["post_processing"]}
        for i, operation in enumerate(self.operation_list):
            data = operation(data)
            pp_history[i+1] = data["metadata"]["post_processing"]
        data["metadata"]["post_processing"] = pp_history
        return data
    
    @property
    def info(self):
        if self.pull_first:
            print(self.init_operation)
        for operation in self.operation_list:
            print(operation)
    
    def __add__(self, ob):
        assert isinstance(ob, self.__class__)
        assert not ob.pull_first, \
            "Only first pipeline can have Pull operation"
        if self.pull_first:
            first_operation = [self.init_operation]
        else:
            first_operation = []
        new_operation_list = first_operation \
                           + self.operation_list \
                           + ob.operation_list

        return Pipeline(new_operation_list)
    