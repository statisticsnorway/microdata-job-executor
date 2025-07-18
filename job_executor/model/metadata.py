from typing import List, Optional, Union

from job_executor.exception import PatchingError, MetadataException
from job_executor.model.camelcase_model import CamelModel

DATA_TYPES_MAPPING = {
    "STRING": "String",
    "LONG": "Long",
    "DOUBLE": "Double",
    "DATE": "Instant",
}

DATA_TYPES_SIKT_TO_SSB = {v: k for k, v in DATA_TYPES_MAPPING.items()}


class TimePeriod(CamelModel):
    start: Union[int, None]
    stop: Optional[Union[int, None]] = None

    def __eq__(self, other):
        return self.start == other.start and self.stop == other.stop

    def __ne__(self, other):
        return not self == other


class KeyType(CamelModel):
    name: str
    label: str
    description: str


class CodeListItem(CamelModel):
    category: str
    code: str

    def patch(self, other: "CodeListItem"):
        if other is None:
            raise PatchingError("Can not delete CodeListItem")
        if self.code != other.code:
            raise PatchingError(
                "Can not change CodeListItem code from "
                f'"{self.code}" to "{other.code}"'
            )
        return CodeListItem(category=other.category, code=self.code)


class ValueDomain(CamelModel):
    description: Optional[str] = None
    unit_of_measure: Optional[str] = None
    code_list: Optional[List[CodeListItem]] = None
    missing_values: Optional[List[str]] = None

    def is_enumerated_value_domain(self):
        return (
            self.code_list is not None
            and self.description is None
            and self.unit_of_measure is None
        )

    def is_described_value_domain(self):
        return (
            self.description is not None
            and self.unit_of_measure is not None
            and self.code_list is None
            and self.missing_values is None
        )

    def patch(self, other: "ValueDomain"):
        patched = {}
        if other is None:
            raise PatchingError("Can not delete ValueDomain")
        if self.is_described_value_domain():
            patched.update({"description": other.description})
            if other.unit_of_measure is not None:
                patched.update({"unitOfMeasure": other.unit_of_measure})
            return ValueDomain(**patched)
        elif self.is_enumerated_value_domain():
            if other.code_list is None:
                raise PatchingError("Can not delete ValueDomain.codeList")
            if self.missing_values != other.missing_values:
                raise PatchingError(
                    "Can not change ValueDomain.sentinelAndMissingValues from "
                    f'"{self.missing_values}" to "{other.missing_values}"'
                )
            if len(self.code_list) != len(other.code_list):
                raise PatchingError(
                    "Can not add or remove codes from ValueDomain.codeList"
                )
            patched = {"codeList": []}
            if self.missing_values is not None:
                patched.update(
                    {"missingValues": [value for value in self.missing_values]}
                )
            sorted_code_list = sorted(self.code_list, key=lambda key: key.code)
            sorted_other_code_list = sorted(
                other.code_list, key=lambda key: key.code
            )
            for idx, code_item in enumerate(sorted_code_list):
                patched["codeList"].append(
                    code_item.patch(sorted_other_code_list[idx]).model_dump(
                        by_alias=True, exclude_none=True
                    )
                )
            return ValueDomain(**patched)
        else:
            raise MetadataException("Invalid ValueDomain")


class RepresentedVariable(CamelModel):
    description: str
    valid_period: TimePeriod
    value_domain: ValueDomain

    def patch(self, other: "RepresentedVariable"):
        is_enumerated = self.value_domain.is_enumerated_value_domain()
        if is_enumerated:
            if self.valid_period != other.valid_period:
                raise PatchingError("Can not change codeList time span")

        return RepresentedVariable(
            **{
                "description": other.description,
                "validPeriod": self.valid_period.model_dump(
                    by_alias=True, exclude_none=True
                ),
                "valueDomain": self.value_domain.patch(
                    other.value_domain
                ).model_dump(by_alias=True, exclude_none=True),
            }
        )

    def patch_description(self, description: str):
        return RepresentedVariable(
            **{
                "description": description,
                "validPeriod": self.valid_period.model_dump(
                    by_alias=True, exclude_none=True
                ),
                "valueDomain": self.value_domain.model_dump(
                    by_alias=True, exclude_none=True
                ),
            }
        )


class Variable(CamelModel):
    name: str
    label: str
    not_pseudonym: bool
    data_type: str
    format: Optional[str] = None
    variable_role: str
    key_type: Optional[KeyType] = None
    represented_variables: List[RepresentedVariable]

    def get_key_type_name(self) -> str | None:
        return None if self.key_type is None else self.key_type.name

    def validate_patching_fields(
        self, other, with_name: bool = False, with_key_type: bool = False
    ):
        caption = "Illegal change to one of these variable fields: \n"
        message = ""

        if self.data_type != other.data_type:
            message += (
                f"dataType: {DATA_TYPES_SIKT_TO_SSB.get(self.data_type)}"
                f" to {DATA_TYPES_SIKT_TO_SSB.get(other.data_type)},"
            )
        if self.format != other.format:
            message += f"format: {self.format} to {other.format},"
        if self.variable_role != other.variable_role:
            message += (
                f"variable_role: {self.variable_role} to "
                f"{other.variable_role}\n"
            )
        if with_name and self.name != other.name:
            message += f"shortName: {self.name} to {other.name}\n"
        if with_key_type and self.key_type.name != other.key_type.name:
            message += (
                f"unitType.name: {self.key_type.name} to {other.key_type.name}"
            )

        if message:
            raise PatchingError(caption + message)

    def validate_represented_variables(self, other: "Variable"):
        if len(self.represented_variables) != len(other.represented_variables):
            raise PatchingError(
                "Can not change the number of code list time periods "
                f"from {len(self.represented_variables)} "
                f"to {len(other.represented_variables)}"
            )


class IdentifierVariable(Variable):
    def patch(self, other: "Variable") -> "Variable":
        if other is None:
            raise PatchingError("Can not delete Variable")

        if self.key_type.name != other.key_type.name:
            raise PatchingError(
                "Can not change Identifier unitType from "
                f"{self.key_type.name} to {other.key_type.name}"
            )

        # Centralized variable definition was used,
        # don't patch the one that is in the datastore.
        # The definition might have changed and we don't want to update it
        # - one needs to use CHANGE operation for that.
        return self


class MeasureVariable(Variable):
    def patch(self, other: "Variable") -> "Variable":
        centralized_variable_definition = False
        if other is None:
            raise PatchingError("Can not delete Variable")
        if self.key_type is not None:
            if other.key_type is None:
                raise PatchingError(
                    f"Can not remove unitType: {self.key_type}"
                )
            # Centralized variable definition was used,
            # it is safe to only patch label and description fields.
            self.validate_patching_fields(other, with_key_type=True)
            centralized_variable_definition = True
        else:
            if other.key_type is not None:
                raise PatchingError(f"Can not add unitType: {other.key_type}")
            self.validate_patching_fields(other, with_name=True)
            self.validate_represented_variables(other)

        patched = {}
        patched_represented_variables = []

        if centralized_variable_definition:
            description = other.represented_variables[0].description
            patched_represented_variables = [
                represented.patch_description(description)
                for represented in self.represented_variables
            ]
        else:
            for idx, _ in enumerate(self.represented_variables):
                patched_represented_variables.append(
                    self.represented_variables[idx]
                    .patch(other.represented_variables[idx])
                    .model_dump(by_alias=True, exclude_none=True)
                )
        patched.update(
            {
                "name": self.name,
                "label": other.label,
                "notPseudonym": self.not_pseudonym,
                "dataType": self.data_type,
                "variableRole": self.variable_role,
                "representedVariables": patched_represented_variables,
            }
        )
        if self.format is not None:
            patched.update({"format": self.format})
        if centralized_variable_definition:
            patched.update(
                {
                    "keyType": self.key_type.model_dump(
                        by_alias=True, exclude_none=True
                    )
                }
            )
        return Variable(**patched)


class AttributeVariable(Variable): ...


class TemporalEnd(CamelModel):
    description: str
    successors: Optional[List[str]] = None


class Metadata(CamelModel):
    name: str
    temporality: str
    language_code: str
    sensitivity_level: str
    population_description: str
    subject_fields: List[str]
    temporal_coverage: TimePeriod
    measure_variable: MeasureVariable
    identifier_variables: List[IdentifierVariable]
    attribute_variables: List[AttributeVariable]
    temporal_status_dates: Optional[List[int]] = None
    temporal_end: Optional[TemporalEnd] = None

    def get_identifier_key_type_name(self) -> str | None:
        return self.identifier_variables[0].get_key_type_name()

    def get_measure_key_type_name(self):
        return self.measure_variable.get_key_type_name()

    def patch(self, other: "Metadata") -> "Metadata":
        if other is None:
            raise PatchingError("Can not patch with NoneType Metadata")
        if (
            self.name != other.name
            or self.temporality != other.temporality
            or self.language_code != other.language_code
        ):
            raise PatchingError(
                "Can not change these metadata fields "
                "[shortName, temporalityType, languageCode]"
            )
        if len(self.attribute_variables) != len(other.attribute_variables):
            raise PatchingError("Can not delete or add attributeVariables")

        if self.sensitivity_level != other.sensitivity_level:
            raise PatchingError("Can not change sensitivityLevel")

        metadata_dict = {
            "name": self.name,
            "temporality": self.temporality,
            "languageCode": self.language_code,
            "sensitivityLevel": self.sensitivity_level,
            "populationDescription": other.population_description,
            "subjectFields": [field for field in other.subject_fields],
            "temporalCoverage": self.temporal_coverage.model_dump(
                by_alias=True, exclude_none=True
            ),
            "measureVariable": (
                self.measure_variable.patch(other.measure_variable).model_dump(
                    by_alias=True, exclude_none=True
                )
            ),
            "identifierVariables": [
                self.identifier_variables[0]
                .patch(other.identifier_variables[0])
                .model_dump(by_alias=True, exclude_none=True)
            ],
            "attributeVariables": self.attribute_variables,
            "temporalStatusDates": self.temporal_status_dates,
        }
        if other.temporal_end is not None:
            metadata_dict["temporalEnd"] = other.temporal_end.model_dump(
                by_alias=True, exclude_none=True
            )
        if self.temporal_status_dates is None:
            del metadata_dict["temporalStatusDates"]
        return Metadata(**metadata_dict)
