import logging
from datetime import datetime, timezone

from job_executor.adapter.local_storage.models.metadata import (
    DATA_TYPES_MAPPING,
)
from job_executor.exception import BuilderStepError

logger = logging.getLogger()


def _get_norwegian_text(multi_language_subject_field: list[dict]) -> str:
    return next(
        language
        for language in multi_language_subject_field
        if language["languageCode"] == "no"
    )["value"]


def _days_since_epoch(date_string: str) -> int:
    epoch = datetime.fromtimestamp(0, tz=timezone.utc)
    date_obj = datetime.fromisoformat(date_string).replace(tzinfo=timezone.utc)
    return (date_obj - epoch).days


def _get_variable_role(attribute_type: str) -> str:
    return {"stop": "Stop", "start": "Start", "source": "Source"}.get(
        attribute_type.lower(), attribute_type
    )


def _get_temporal_coverage(start: str | None, stop: str | None) -> dict:
    period = {"start": start if start is None else _days_since_epoch(start)}
    if stop:
        period["stop"] = _days_since_epoch(stop)
    return period


def _transform_data_type(data_type: str) -> str:
    return DATA_TYPES_MAPPING.get(data_type, data_type)


def _transform_temporal_status_dates(status_dates: list | None) -> list | None:
    return (
        None
        if status_dates is None
        else [_days_since_epoch(status_date) for status_date in status_dates]
    )


def _transform_subject_fields(subject_fields: list[list[dict]]) -> list[str]:
    return [
        _get_norwegian_text(subject_field) for subject_field in subject_fields
    ]


def _represented_variables_from_description(
    description: str, value_domain: dict, start: str, stop: str
) -> list:
    return [
        {
            "description": description,
            "validPeriod": {
                "start": (start if start is None else _days_since_epoch(start)),
                "stop": (stop if stop is None else _days_since_epoch(stop)),
            },
            "valueDomain": {
                "description": _get_norwegian_text(value_domain["description"]),
                "unitOfMeasure": _get_norwegian_text(
                    value_domain.get(
                        "measurementUnitDescription",
                        [{"languageCode": "no", "value": "N/A"}],
                    )
                ),
            },
        }
    ]


def _represented_variables_from_code_list(
    description: str,
    sentinel_and_missing_values: list,
    code_items: list,
) -> list:
    if not code_items:
        raise ValueError("Code list can not be empty")

    ONE_DAY = 1

    valid_from_dates = [
        _days_since_epoch(item["validFrom"])
        for item in code_items
        if "validFrom" in item
    ]
    valid_until_dates = [
        _days_since_epoch(item["validUntil"]) + ONE_DAY
        for item in code_items
        if item.get("validUntil", None) is not None
    ]
    has_ongoing_time_period = any(
        [item.get("validUntil", None) is None for item in code_items]
    )
    unique_dates = list(set(valid_from_dates + valid_until_dates))
    unique_dates.sort()

    valid_periods = []
    for i, _ in enumerate(unique_dates):
        valid_period = {"start": unique_dates[i]}
        if i < len(unique_dates) - 1:
            valid_period["stop"] = unique_dates[i + 1] - ONE_DAY
        valid_periods.append(valid_period)

    if not has_ongoing_time_period:
        valid_periods = valid_periods[:-1]

    represented_variables = []
    for valid_period in valid_periods:
        codeList = []
        for code_item in code_items:
            code_valid_from = _days_since_epoch(code_item["validFrom"])
            code_valid_until = (
                _days_since_epoch(code_item["validUntil"])
                if code_item.get("validUntil", None) is not None
                else None
            )
            valid_period_is_ongoing: bool = "stop" not in valid_period
            code_period_is_ongoing: bool = code_valid_until is None
            code_period_started_before_valid_period: bool = (
                code_valid_from <= valid_period["start"]
            )
            valid_period_inside_code_period: bool = (
                not valid_period_is_ongoing
                and not code_period_is_ongoing
                and code_valid_from <= valid_period["start"]
                and valid_period["stop"] <= code_valid_until
            )
            code_in_valid_period: bool = (
                code_period_is_ongoing
                and code_period_started_before_valid_period
            ) or (valid_period_inside_code_period)
            if code_in_valid_period:
                codeList.append(
                    {
                        "category": _get_norwegian_text(
                            code_item["categoryTitle"]
                        ),
                        "code": code_item["code"],
                    }
                )
        if sentinel_and_missing_values:
            for code_item in sentinel_and_missing_values:
                codeList.append(
                    {
                        "category": _get_norwegian_text(
                            code_item["categoryTitle"]
                        ),
                        "code": code_item["code"],
                    }
                )
        represented_variables.append(
            {
                "description": description,
                "validPeriod": valid_period,
                "valueDomain": {
                    "codeList": codeList,
                    "missingValues": [
                        value["code"] for value in sentinel_and_missing_values
                    ],
                },
            }
        )
    return represented_variables


def _create_represented_variables(
    description: str,
    value_domain: dict,
    temporal_coverage_start: str,
    temporal_coverage_latest: str,
) -> list:
    sentinel_and_missing_values = value_domain.get(
        "sentinelAndMissingValues", []
    )
    if "codeList" in value_domain:
        return _represented_variables_from_code_list(
            description=description,
            sentinel_and_missing_values=sentinel_and_missing_values,
            code_items=value_domain["codeList"],
        )
    else:
        return _represented_variables_from_description(
            description=description,
            value_domain=value_domain,
            start=temporal_coverage_start,
            stop=temporal_coverage_latest,
        )


def _transform_variable(
    variable: dict, role: str, start: str, stop: str
) -> dict:
    variable_description = (
        _get_norwegian_text(variable["description"])
        if "description" in variable
        else "N/A"
    )
    not_pseudonym = (
        "unitType" not in variable
        or not variable["unitType"]["requiresPseudonymization"]
    )
    transformed_variable = {
        "variableRole": role,
        "name": variable["shortName"],
        "label": _get_norwegian_text(variable["name"]),
        "notPseudonym": not_pseudonym,
        "dataType": (
            _transform_data_type(variable["dataType"])
            if not_pseudonym
            else "Long"
        ),
        "representedVariables": _create_represented_variables(
            description=variable_description,
            value_domain=variable["valueDomain"],
            temporal_coverage_start=start,
            temporal_coverage_latest=stop,
        ),
    }
    if "format" in variable:
        transformed_variable["format"] = variable["format"]
    if "unitType" in variable:
        transformed_variable["keyType"] = {
            "name": variable["unitType"]["shortName"],
            "label": _get_norwegian_text(variable["unitType"]["name"]),
            "description": _get_norwegian_text(
                variable["unitType"]["description"]
            ),
        }
    return transformed_variable


def _transform_attribute_variables(
    metadata: dict, start: str, stop: str
) -> list[dict]:
    attributes = [
        next(
            (
                variable
                for variable in metadata["attributeVariables"]
                if variable["variableRole"] == "Start"
            ),
            None,
        ),
        next(
            (
                variable
                for variable in metadata["attributeVariables"]
                if variable["variableRole"] == "Stop"
            ),
            None,
        ),
    ]
    return [
        _transform_variable(
            attribute,
            _get_variable_role(attribute["variableRole"]),
            start,
            stop,
        )
        for attribute in attributes
        if attribute is not None
    ]


def _transform_temporal_end(temporal_end: dict) -> dict[str, str]:
    temporal_end_result = {
        "description": _get_norwegian_text(temporal_end["description"])
    }
    if (
        temporal_end.get("successors") is not None
        and len(temporal_end["successors"]) > 0
    ):
        temporal_end_result["successors"] = temporal_end["successors"]
    return temporal_end_result


def _transform_metadata(metadata: dict) -> dict:
    logger.info("Transforming metadata")
    # These values are found by going through the data file.
    # When we do transformation of metadata alone, we do not
    # have these fields and choose to ignore them.
    # They are in that case NOT used to update metadata state.
    start = metadata["dataRevision"].get("temporalCoverageStart", None)
    stop = metadata["dataRevision"].get("temporalCoverageLatest", None)

    transformed_identifiers = [
        _transform_variable(identifier, "Identifier", start, stop)
        for identifier in metadata["identifierVariables"]
    ]
    transformed_measure = _transform_variable(
        metadata["measureVariables"][0], "Measure", start, stop
    )
    transformed_attributes = _transform_attribute_variables(
        metadata, start, stop
    )

    transformed = {
        "name": metadata["shortName"],
        "populationDescription": _get_norwegian_text(
            metadata["populationDescription"]
        ),
        "languageCode": "no",
        "temporality": metadata["temporalityType"],
        "sensitivityLevel": metadata["sensitivityLevel"],
        "subjectFields": _transform_subject_fields(metadata["subjectFields"]),
        "temporalCoverage": _get_temporal_coverage(start, stop),
        "identifierVariables": transformed_identifiers,
        "measureVariable": transformed_measure,
        "attributeVariables": transformed_attributes,
    }
    if metadata["temporalityType"] == "STATUS":
        transformed["temporalStatusDates"] = _transform_temporal_status_dates(
            metadata["dataRevision"].get("temporalStatusDates", None)
        )
    if metadata["dataRevision"].get("temporalEnd") is not None:
        transformed["temporalEnd"] = _transform_temporal_end(
            metadata["dataRevision"]["temporalEnd"]
        )
    logger.info("Finished transformation")
    return transformed


def run(metadata: dict) -> dict:
    """
    Transforms a metadatafile from the input model to the SIKT
    metadata model that is stored in the datastore.
    Returns the path of the transformed metadata file.
    """
    try:
        return _transform_metadata(metadata)
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise BuilderStepError("Failed to transform dataset") from e
