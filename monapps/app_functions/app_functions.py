from .stall_detection_by_two_temps import stall_detection_by_two_temps_0_0_1
from .fake_data_generator import fake_data_generator_0_0_1
from .monitoring import monitoring_0_0_1


app_function_map = {
    "stall_detection_by_two_temps": {"0.0.1": stall_detection_by_two_temps_0_0_1},
    "fake_data_generator": {"0.0.1": fake_data_generator_0_0_1},
    "monitoring": {"0.0.1": monitoring_0_0_1},
}
