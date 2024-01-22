from kfptest import TestStep
from example_components import non_named_output_component


def test_local_env_non_named_component(tmp_path):
    component_args = {"input_list": [1,2,3,4,5]}
    test_step = TestStep(non_named_output_component, component_args, tmp_path)
    test_run = test_step.run()
    assert test_run.obj["squared_list"] == [1, 4, 9, 16, 25]
