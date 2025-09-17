import json
from pathlib import Path

from microdata_tools.validation.components import (
    temporal_attributes,
    unit_type_variables,
)

INPUT_DATA_PATH = Path("tests/resources/worker/steps/transformer/input_data")
EXPECTED_DATA_PATH = Path("tests/resources/worker/steps/transformer/expected")

PERSON_IDENTIFIER = unit_type_variables.get("PERSON")
BK_HELSTASJONSKONSULTASJON_IDENTIFIER = unit_type_variables.get(
    "BK_HELSESTASJONSKONSULTASJON"
)

KREFTREG_DS_DESCRIBED = json.load(
    open(INPUT_DATA_PATH / "KREFTREG_DS_described.json")
)
KREFTREG_DS_DESCRIBED["identifierVariables"] = [
    unit_type_variables.get("PERSON")
]
KREFTREG_DS_DESCRIBED["attributeVariables"] = [
    temporal_attributes.generate_start_time_attribute("STATUS"),
    temporal_attributes.generate_stop_time_attribute("STATUS"),
]

KREFTREG_DS_ENUMERATED = json.load(
    open(INPUT_DATA_PATH / "KREFTREG_DS_enumerated.json")
)
KREFTREG_DS_ENUMERATED["identifierVariables"] = [
    unit_type_variables.get("PERSON")
]
KREFTREG_DS_ENUMERATED["attributeVariables"] = [
    temporal_attributes.generate_start_time_attribute("STATUS"),
    temporal_attributes.generate_stop_time_attribute("STATUS"),
]

UTDANNING_PATCH = json.load(open(INPUT_DATA_PATH / "UTDANNING_PATCH.json"))
UTDANNING_PATCH["identifierVariables"] = [unit_type_variables.get("PERSON")]
UTDANNING_PATCH["attributeVariables"] = [
    temporal_attributes.generate_start_time_attribute("STATUS"),
    temporal_attributes.generate_stop_time_attribute("STATUS"),
]

UTDANNING = json.load(open(INPUT_DATA_PATH / "UTDANNING.json"))
UTDANNING["identifierVariables"] = [unit_type_variables.get("PERSON")]
UTDANNING["attributeVariables"] = [
    temporal_attributes.generate_start_time_attribute("STATUS"),
    temporal_attributes.generate_stop_time_attribute("STATUS"),
]

DESCRIBED_EXPECTED = json.load(
    open(EXPECTED_DATA_PATH / "KREFTREG_DS_described.json")
)
STATUS_EXPECTED = json.load(open(EXPECTED_DATA_PATH / "UTDANNING.json"))
STATUS_PATCH_EXPECTED = json.load(
    open(EXPECTED_DATA_PATH / "UTDANNING_PATCH.json")
)
ENUMERATED_EXPECTED = json.load(
    open(EXPECTED_DATA_PATH / "KREFTREG_DS_enumerated.json")
)

CODELIST = [
    {
        "code": "1",
        "categoryTitle": [{"languageCode": "no", "value": "Grunnskole"}],
        "validFrom": "1900-01-01",
        "validUntil": None,
    },
    {
        "code": "2",
        "categoryTitle": [{"languageCode": "no", "value": "Gymnasium"}],
        "validFrom": "1910-01-01",
        "validUntil": "1919-12-31",
    },
    {
        "code": "3",
        "categoryTitle": [{"languageCode": "no", "value": "Bachelorgrad"}],
        "validFrom": "1910-01-01",
        "validUntil": None,
    },
    {
        "code": "2",
        "categoryTitle": [
            {"languageCode": "no", "value": "Videregående skole"}
        ],
        "validFrom": "1920-01-01",
        "validUntil": None,
    },
    {
        "code": "4",
        "categoryTitle": [{"languageCode": "no", "value": "Mastergrad"}],
        "validFrom": "1940-01-01",
        "validUntil": None,
    },
    {
        "code": "5",
        "categoryTitle": [{"languageCode": "no", "value": "Doktorgrad"}],
        "validFrom": "1940-01-01",
        "validUntil": None,
    },
]

MISSING_VALUES = [
    {
        "code": "99",
        "categoryTitle": [{"languageCode": "no", "value": "Ukjent"}],
    }
]

TRANSFORMED_CODELIST = [
    {
        "description": "description",
        "validPeriod": {"start": -25567, "stop": -21916},
        "valueDomain": {
            "codeList": [{"category": "Grunnskole", "code": "1"}],
            "missingValues": [],
        },
    },
    {
        "description": "description",
        "validPeriod": {"start": -21915, "stop": -18264},
        "valueDomain": {
            "codeList": [
                {"category": "Grunnskole", "code": "1"},
                {"category": "Gymnasium", "code": "2"},
                {"category": "Bachelorgrad", "code": "3"},
            ],
            "missingValues": [],
        },
    },
    {
        "description": "description",
        "validPeriod": {"start": -18263, "stop": -10959},
        "valueDomain": {
            "codeList": [
                {"category": "Grunnskole", "code": "1"},
                {"category": "Bachelorgrad", "code": "3"},
                {"category": "Videregående skole", "code": "2"},
            ],
            "missingValues": [],
        },
    },
    {
        "description": "description",
        "validPeriod": {"start": -10958},
        "valueDomain": {
            "codeList": [
                {"category": "Grunnskole", "code": "1"},
                {"category": "Bachelorgrad", "code": "3"},
                {"category": "Videregående skole", "code": "2"},
                {"category": "Mastergrad", "code": "4"},
                {"category": "Doktorgrad", "code": "5"},
            ],
            "missingValues": [],
        },
    },
]

TRANSFORMED_CODELIST_WITH_MISSING_VALUES = [
    {
        **represented,
        "valueDomain": {
            "codeList": represented["valueDomain"]["codeList"]
            + [{"category": "Ukjent", "code": "99"}],
            "missingValues": ["99"],
        },
    }
    for represented in TRANSFORMED_CODELIST
]
