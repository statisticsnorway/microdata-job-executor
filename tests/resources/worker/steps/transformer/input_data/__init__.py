import json
from pathlib import Path

from microdata_tools.validation.components import (
    unit_type_variables,
    temporal_attributes,
)


TEST_DATA_PATH = Path("tests/resources/worker/steps/transformer/input_data")

KREFTREG_DS_DESCRIBED = json.load(
    open(TEST_DATA_PATH / "KREFTREG_DS_described.json")
)
KREFTREG_DS_DESCRIBED["identifierVariables"] = [
    unit_type_variables.get("PERSON")
]
KREFTREG_DS_DESCRIBED["attributeVariables"] = [
    temporal_attributes.generate_start_time_attribute("STATUS"),
    temporal_attributes.generate_stop_time_attribute("STATUS"),
]

KREFTREG_DS_ENUMERATED = json.load(
    open(TEST_DATA_PATH / "KREFTREG_DS_enumerated.json")
)
KREFTREG_DS_ENUMERATED["identifierVariables"] = [
    unit_type_variables.get("PERSON")
]
KREFTREG_DS_ENUMERATED["attributeVariables"] = [
    temporal_attributes.generate_start_time_attribute("STATUS"),
    temporal_attributes.generate_stop_time_attribute("STATUS"),
]

UTDANNING_PATCH = json.load(open(TEST_DATA_PATH / "UTDANNING_PATCH.json"))
UTDANNING_PATCH["identifierVariables"] = [unit_type_variables.get("PERSON")]
UTDANNING_PATCH["attributeVariables"] = [
    temporal_attributes.generate_start_time_attribute("STATUS"),
    temporal_attributes.generate_stop_time_attribute("STATUS"),
]

UTDANNING = json.load(open(TEST_DATA_PATH / "UTDANNING.json"))
UTDANNING["identifierVariables"] = [unit_type_variables.get("PERSON")]
UTDANNING["attributeVariables"] = [
    temporal_attributes.generate_start_time_attribute("STATUS"),
    temporal_attributes.generate_stop_time_attribute("STATUS"),
]
