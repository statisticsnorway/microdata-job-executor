from typing import List, Optional, Union
from pydantic import BaseModel, root_validator

from job_executor.exception.exception import (
    PatchingError,
    MetadataException
)


class TimePeriod(BaseModel):
    start: Union[int, None]
    stop: Optional[Union[int, None]]

    def dict(self, **kwargs) -> dict:  # pylint: disable=unused-argument
        return (
            {"start": self.start, "stop": self.stop}
            if self.stop is not None
            else {"start": self.start}
        )


class KeyType(BaseModel):
    name: str
    label: str
    description: str

    def patch(self, other: 'KeyType'):
        if other is None:
            raise PatchingError(
                'Can not delete KeyType'
            )
        if self.name != other.name:
            raise PatchingError(
                'Can not change keyType name from '
                f'"{self.name}" to "{other.name}"'
            )
        return KeyType(**{
            'name': self.name,
            'label': other.label,
            'description': other.description
        })


class CodeListItem(BaseModel):
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


class ValueDomain(BaseModel):
    description: Optional[str]
    unitOfMeasure: Optional[str]
    codeList: Optional[List[CodeListItem]]
    missingValues: Optional[List[str]]

    def _is_enumerated_value_domain(self):
        return (
            self.codeList is not None
            and self.description is None
            and self.unitOfMeasure is None
        )

    def _is_described_value_domain(self):
        return (
            self.description is not None
            and self.unitOfMeasure is not None
            and self.codeList is None
            and self.missingValues is None
        )

    def dict(self, **kwargs) -> dict:  # pylint: disable=unused-argument
        if self._is_described_value_domain():
            return {
                key: value for key, value in {
                    "description": self.description,
                    "unitOfMeasure": self.unitOfMeasure
                }.items() if value is not None
            }
        elif self._is_enumerated_value_domain():
            return {
                "codeList": [
                    code_item.dict() for code_item in self.codeList
                ],
                "missingValues": [
                    missing_value for missing_value
                    in self.missingValues
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
            if other.unitOfMeasure is not None:
                patched.update({'unitOfMeasure': other.unitOfMeasure})
            return ValueDomain(**patched)
        elif self._is_enumerated_value_domain():
            if other.codeList is None:
                raise PatchingError(
                    'Can not delete code list'
                )
            if self.missingValues != other.missingValues:
                raise PatchingError(
                    'Can not change ValueDomain missingValues from '
                    f'"{self.missingValues}" to "{other.missingValues}"'
                )
            if len(self.codeList) != len(other.codeList):
                raise PatchingError(
                    'Can not add or remove codes from ValueDomain codeList'
                )
            patched = {
                'codeList': []
            }
            if self.missingValues is not None:
                patched.update({
                    'missingValues': [value for value in self.missingValues]
                })
            for idx, _ in enumerate(self.codeList):
                patched['codeList'].append(
                    self.codeList[idx].patch(other.codeList[idx]).dict()
                )
            return ValueDomain(**patched)
        else:
            raise MetadataException('Invalid ValueDomain')


class RepresentedVariable(BaseModel):
    description: str
    validPeriod: TimePeriod
    valueDomain: ValueDomain

    def patch(self, other: 'RepresentedVariable'):
        if other is None:
            raise PatchingError(
                'Can not delete RepresentedVariable'
            )
        return RepresentedVariable(**{
            "description": other.description,
            "validPeriod": self.validPeriod.dict(),
            "valueDomain": self.valueDomain.patch(other.valueDomain).dict()
        })


class Variable(BaseModel):
    name: str
    label: str
    dataType: str
    format: Optional[str]
    variableRole: str
    keyType: Optional[KeyType]
    representedVariables: List[RepresentedVariable]

    @root_validator(pre=True)
    @classmethod
    def remove_none(cls, values):
        return {
            key: value for key, value in values.items()
            if value is not None
        }

    def get_key_type_name(self):
        return None if self.keyType is None else self.keyType.name

    def dict(self, **kwargs) -> dict:  # pylint: disable=unused-argument
        dict_representation = {
            "name": self.name,
            "label": self.label,
            "dataType": self.dataType,
            "variableRole": self.variableRole,
            "representedVariables": [
                represented_variable.dict()
                for represented_variable in self.representedVariables
            ]
        }
        if self.format is not None:
            dict_representation["format"] = self.format
        if self.keyType is not None:
            dict_representation["keyType"] = self.keyType.dict()
        return dict_representation

    def patch(self, other: 'Variable'):
        patched = {}
        if other is None:
            raise PatchingError(
                'Can not delete Variable'
            )
        if (
            self.name != other.name or
            self.dataType != other.dataType or
            self.format != other.format or
            self.variableRole != other.variableRole
        ):
            raise PatchingError(
                'Illegal change to one of these variable fields: '
                '[name, dataType, format, variableRole]]'
            )
        if self.keyType is None and other.keyType is not None:
            raise PatchingError('Can not change keyType')
        if len(self.representedVariables) != len(other.representedVariables):
            raise PatchingError(
                'Can not add or delete represented variables.'
            )
        patched_represented_variables = []
        for idx, _ in enumerate(self.representedVariables):
            patched_represented_variables.append(
                self.representedVariables[idx].patch(
                    other.representedVariables[idx]
                ).dict()
            )
        patched.update({
            "name": self.name,
            "label": other.label,
            "dataType": self.dataType,
            "variableRole": self.variableRole,
            "representedVariables": patched_represented_variables
        })
        if self.format is not None:
            patched.update({"format": self.format})
        if self.keyType is not None:
            patched.update({
                'keyType': self.keyType.patch(other.keyType).dict()
            })
        return Variable(**patched)


class IdentifierVariable(Variable):
    pass


class MeasureVariable(Variable):
    pass


class AttributeVariable(Variable):
    pass


class Metadata(BaseModel):
    name: str
    temporality: str
    languageCode: str
    populationDescription: str
    subjectFields: List[str]
    temporalCoverage: TimePeriod
    measureVariable: MeasureVariable
    identifierVariables: List[IdentifierVariable]
    attributeVariables: List[AttributeVariable]

    def get_identifier_key_type_name(self):
        return self.identifierVariables[0].get_key_type_name()

    def get_measure_key_type_name(self):
        return self.measureVariable.get_key_type_name()

    def patch(self, other: 'Metadata') -> 'Metadata':
        if other is None:
            raise PatchingError(
                'Can not patch with NoneType Metadata'
            )
        if (
            self.name != other.name or
            self.temporality != other.temporality or
            self.languageCode != other.languageCode
        ):
            raise PatchingError(
                'Can not change these metadata fields '
                '[name, temporality, languageCode]'
            )
        if len(self.attributeVariables) != len(other.attributeVariables):
            raise PatchingError('Can not delete or add attributeVariables')
        patched_attribute_variables = []
        for idx, _ in enumerate(self.attributeVariables):
            patched_attribute_variables.append(
                self.attributeVariables[idx].patch(
                    other.attributeVariables[idx]
                ).dict()
            )
        return Metadata(**{
            "name": self.name,
            "temporality": self.temporality,
            "languageCode": self.languageCode,
            "populationDescription": other.populationDescription,
            "subjectFields": [field for field in other.subjectFields],
            "temporalCoverage": self.temporalCoverage.dict(),
            "measureVariable": (
                self.measureVariable.patch(other.measureVariable).dict()
            ),
            "identifierVariables": [
                self.identifierVariables[0].patch(
                    other.identifierVariables[0]
                ).dict()
            ],
            "attributeVariables": patched_attribute_variables
        })
