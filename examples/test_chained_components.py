from example_components import chained_component_1, chained_component_2
from kfptest import TestStep


def test_chained_components(tmp_path):
    component_1_args = {"input_list": [1,2,3,4,5]}
    step_1 = TestStep(chained_component_1, component_1_args, tmp_path)
    step_1_run = step_1.run()

    component_2_args = {"squared_list": step_1_run.obj["squared_list"]}
    step_2 = TestStep(chained_component_2, component_2_args, tmp_path)
    step_2_run = step_2.run()
    assert step_2_run.obj["halved_list"] == [0.5, 2, 4.5, 8, 12.5]
