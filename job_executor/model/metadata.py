from typing import List, Optional, Union
from pydantic import root_validator

from job_executor.model.camelcase_model import CamelModel
from job_executor.exception import (
    PatchingError,
    MetadataException
)


class TimePeriod(CamelModel):
    start: Union[int, None]
    stop: Optional[Union[int, None]]

    def dict(self, **kwargs) -> dict:  # pylint: disable=unused-argument
        return (
            {"start": self.start, "stop": self.stop}
            if self.stop is not None
            else {"start": self.start}
        )


class KeyType(CamelModel):
    name: str
    label: str
    description: str

    def patch(self, other: 'KeyType'):
        if other is None:
            raise PatchingError(
                'Can not delete UnitType'
            )
        if self.name != other.name:
            raise PatchingError(
                'Can not change UnitType.shortName from '
                f'"{self.name}" to "{other.name}"'
            )
        return KeyType(**{
            'name': self.name,
            'label': other.label,
            'description': other.description
        })


class CodeListItem(CamelModel):
    category: str
    code: str

    def patch(self, other: 'CodeListItem'):
        if other is None:
            raise PatchingError(
                'Can not delete CodeListItem'
            )
        if self.code != other.code:
            raise PatchingError(
                'Can not change CodeListItem code from '
                f'"{self.code}" to "{other.code}"'
            )
        return CodeListItem(
            category=other.category,
            code=self.code
        )


class ValueDomain(CamelModel):
    description: Optional[str]
    unit_of_measure: Optional[str]
    code_list: Optional[List[CodeListItem]]
    missing_values: Optional[List[str]]

    def _is_enumerated_value_domain(self):
        return (
            self.code_list is not None
            and self.description is None
            and self.unit_of_measure is None
        )

    def _is_described_value_domain(self):
        return (
            self.description is not None
            and self.unit_of_measure is not None
            and self.code_list is None
            and self.missing_values is None
        )

    def dict(self, **kwargs) -> dict:  # pylint: disable=unused-argument
        if self._is_described_value_domain():
            return {
                key: value for key, value in {
                    "description": self.description,
                    "unitOfMeasure": self.unit_of_measure
                }.items() if value is not None
            }
        elif self._is_enumerated_value_domain():
            return {
                "codeList": [
                    code_item.dict() for code_item in self.code_list
                ],
                "missingValues": [
                    missing_value for missing_value
                    in self.missing_values
                ]
            }
        else:
            MetadataException('Invalid ValueDomain')

    def patch(self, other: 'ValueDomain'):
        patched = {}
        if other is None:
            raise PatchingError(
                'Can not delete ValueDomain'
            )
        if self._is_described_value_domain():
            patched.update({'description': other.description})
            if other.unit_of_measure is not None:
                patched.update({'unitOfMeasure': other.unit_of_measure})
            return ValueDomain(**patched)
        elif self._is_enumerated_value_domain():
            if other.code_list is None:
                raise PatchingError(
                    'Can not delete ValueDomain.codeList'
                )
            if self.missing_values != other.missing_values:
                raise PatchingError(
                    'Can not change ValueDomain.sentinelAndMissingValues from '
                    f'"{self.missing_values}" to "{other.missing_values}"'
                )
            if len(self.code_list) != len(other.code_list):
                raise PatchingError(
                    'Can not add or remove codes from ValueDomain.codeList'
                )
            patched = {
                'codeList': []
            }
            if self.missing_values is not None:
                patched.update({
                    'missingValues': [value for value in self.missing_values]
                })
            for idx, _ in enumerate(self.code_list):
                patched['codeList'].append(
                    self.code_list[idx].patch(
                        other.code_list[idx]
                    ).dict(by_alias=True)
                )
            return ValueDomain(**patched)
        else:
            raise MetadataException('Invalid ValueDomain')


class RepresentedVariable(CamelModel):
    description: str
    valid_period: TimePeriod
    value_domain: ValueDomain

    def patch(self, other: 'RepresentedVariable', only_patch_description: bool):
        if other is None:
            raise PatchingError(
                'Can not delete RepresentedVariable. '
                'Please check valueDomain.codeList field.'
            )
        return RepresentedVariable(**{
            "description": other.description,
            "validPeriod": self.valid_period.dict(by_alias=True),
            "valueDomain": (
                self.value_domain.dict(by_alias=True) if only_patch_description
                else self.value_domain.patch(
                    other.value_domain
                ).dict(by_alias=True)
            )
        })


class Variable(CamelModel):
    name: str
    label: str
    not_pseudonym: bool
    data_type: str
    format: Optional[str]
    variable_role: str
    key_type: Optional[KeyType]
    represented_variables: List[RepresentedVariable]

    @root_validator(pre=True)
    @classmethod
    def remove_none(cls, values):
        return {
            key: value for key, value in values.items()
            if value is not None
        }

    def get_key_type_name(self):
        return None if self.key_type is None else self.key_type.name

    def dict(self, **kwargs) -> dict:  # pylint: disable=unused-argument
        dict_representation = {
            "name": self.name,
            "label": self.label,
            "notPseudonym": self.not_pseudonym,
            "dataType": self.data_type,
            "variableRole": self.variable_role,
            "representedVariables": [
                represented_variable.dict(by_alias=True)
                for represented_variable in self.represented_variables
            ]
        }
        if self.format is not None:
            dict_representation["format"] = self.format
        if self.key_type is not None:
            dict_representation["keyType"] = self.key_type.dict(by_alias=True)
        return dict_representation

    def patch(self, other: 'Variable') -> 'Variable':
        patched = {}
        only_patch_description = False

        if other is None:
            raise PatchingError(
                'Can not delete Variable'
            )

        if self.variable_role == "Identifier":
            # Centralized variable definition was used,
            # don't patch the one that is in the datastore.
            # The definition might have changed and we don't want to update it
            # - one needs to use CHANGE operation for that.
            return self
        if self.variable_role == "Measure" and self.key_type is not None:
            # Centralized variable definition was used,
            # it is safe to only patch label and description fields.
            only_patch_description = True
            self.validate_patching_fields(other, with_key_type=True)
        else:
            self.validate_patching_fields(other, with_name=True)

        self.validate_patching_for_all_variable_roles(other)

        patched_represented_variables = []
        for idx, _ in enumerate(self.represented_variables):
            patched_represented_variables.append(
                self.represented_variables[idx].patch(
                    other.represented_variables[idx],
                    only_patch_description
                ).dict()
            )
        patched.update({
            "name": self.name,
            "label": other.label,
            "notPseudonym": self.not_pseudonym,
            "dataType": self.data_type,
            "variableRole": self.variable_role,
            "representedVariables": patched_represented_variables
        })
        if self.format is not None:
            patched.update({"format": self.format})
        if self.key_type is not None:
            patched.update({
                'keyType': self.key_type.dict() if only_patch_description else
                self.key_type.patch(other.key_type).dict()
            })
        return Variable(**patched)

    def validate_patching_fields(
        self, other, with_name: bool = False, with_key_type: bool = False
    ):
        caption = 'Illegal change to one of these variable fields: \n'
        message = ''
        if (
            self.data_type != other.data_type or
            self.format != other.format or
            self.variable_role != other.variable_role
        ):
            message = (
                f'dataType: {self.data_type} to {other.data_type},'
                f'format: {self.format} to {other.format},'
                f'variable_role: {self.variable_role} to '
                f'{other.variable_role}\n'
            )
        if (
            with_name and
            self.name != other.name
        ):
            message += f'shortName: {self.name} to {other.name}\n'
        if (
            with_key_type and
            self.key_type.name != other.key_type.name
        ):
            message += f'unitType.name: {self.key_type.name} ' \
                       f'to {other.key_type.name}'

        if message:
            raise PatchingError(caption + message)

    def validate_patching_for_all_variable_roles(self, other: 'Variable'):
        if self.key_type is None and other.key_type is not None:
            raise PatchingError('Can not change unitType')
        if len(self.represented_variables) != len(other.represented_variables):
            raise PatchingError(
                'Can not add or delete represented variables. '
                'Please check valueDomain.codeList field.'
            )
        if self.not_pseudonym != other.not_pseudonym:
            raise PatchingError(
                'Can not change unitType''s pseudonym status from '
                f'"{self.not_pseudonym}" to "{other.not_pseudonym}"'
                'Please check unitType.requiresPseudonymization field. '
            )


class IdentifierVariable(Variable):
    ...


class MeasureVariable(Variable):
    ...


class AttributeVariable(Variable):
    ...


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
    temporal_status_dates: Optional[List[int]]

    def get_identifier_key_type_name(self):
        return self.identifier_variables[0].get_key_type_name()

    def get_measure_key_type_name(self):
        return self.measure_variable.get_key_type_name()

    def patch(self, other: 'Metadata') -> 'Metadata':
        if other is None:
            raise PatchingError(
                'Can not patch with NoneType Metadata'
            )
        if (
            self.name != other.name or
            self.temporality != other.temporality or
            self.language_code != other.language_code
        ):
            raise PatchingError(
                'Can not change these metadata fields '
                '[shortName, temporalityType, languageCode]'
            )
        if len(self.attribute_variables) != len(other.attribute_variables):
            raise PatchingError('Can not delete or add attributeVariables')

        if self.sensitivity_level != other.sensitivity_level:
            raise PatchingError('Can not change sensitivityLevel')

        sorted_self_attributes = sorted(
            self.attribute_variables, key=lambda k: k.name
        )
        sorted_other_attributes = sorted(
            other.attribute_variables, key=lambda k: k.name
        )
        patched_attribute_variables = [
            sorted_self_attributes[0].patch(sorted_other_attributes[0]).dict(),
            sorted_self_attributes[1].patch(sorted_other_attributes[1]).dict()
        ]
        metadata_dict = {
            "name": self.name,
            "temporality": self.temporality,
            "languageCode": self.language_code,
            "sensitivityLevel": self.sensitivity_level,
            "populationDescription": other.population_description,
            "subjectFields": [field for field in other.subject_fields],
            "temporalCoverage": self.temporal_coverage.dict(),
            "measureVariable": (
                self.measure_variable.patch(other.measure_variable).dict()
            ),
            "identifierVariables": [
                self.identifier_variables[0].patch(
                    other.identifier_variables[0]
                ).dict()
            ],
            "attributeVariables": patched_attribute_variables,
            "temporalStatusDates": self.temporal_status_dates
        }
        if self.temporal_status_dates is None:
            del metadata_dict['temporalStatusDates']
        return Metadata(**metadata_dict)

    def dict(self, **kwargs) -> dict:  # pylint: disable=unused-argument
        metadata_dict = {
            "name": self.name,
            "temporality": self.temporality,
            "languageCode": self.language_code,
            "sensitivityLevel": self.sensitivity_level,
            "populationDescription": self.population_description,
            "subjectFields": [field for field in self.subject_fields],
            "temporalCoverage": self.temporal_coverage.dict(),
            "measureVariable": self.measure_variable.dict(),
            "identifierVariables": [
                self.identifier_variables[0].dict()
            ],
            "attributeVariables": [
                self.attribute_variables[0].dict(),
                self.attribute_variables[1].dict()
            ],
            "temporalStatusDates": self.temporal_status_dates
        }
        if self.temporal_status_dates is None:
            del metadata_dict['temporalStatusDates']
        return metadata_dict
