import pytest
from pydantic import ValidationError

from job_executor.adapter.local_storage.models.datastore_versions import (
    DataStructureUpdate,
)
from job_executor.common.exceptions import ReleaseStatusException

DATA_STRUCTURE_ADD_UPDATE = {
    "name": "KJOENN",
    "description": "Første publisering",
    "operation": "ADD",
    "releaseStatus": "DRAFT",
}
DATA_STRUCTURE_REMOVE_UPDATE = {
    "name": "KJOENN",
    "description": "Fjernet variabel",
    "operation": "REMOVE",
    "releaseStatus": "PENDING_DELETE",
}
DATA_STRUCTURE_UPDATE_MISSING_FIELD = {
    "name": "KJOENN",
    "description": "Første publisering",
    "operation": "ADD",
}


def test_data_structure_update():
    data_structure_update = DataStructureUpdate(**DATA_STRUCTURE_ADD_UPDATE)
    assert (
        data_structure_update.model_dump(by_alias=True, exclude_none=True)
        == DATA_STRUCTURE_ADD_UPDATE
    )


def test_invalid_data_structure_update():
    with pytest.raises(ValidationError):
        DataStructureUpdate(**DATA_STRUCTURE_UPDATE_MISSING_FIELD)


def test_set_release_status():
    data_structure_update = DataStructureUpdate(**DATA_STRUCTURE_ADD_UPDATE)
    data_structure_update.set_release_status("PENDING_RELEASE")
    assert data_structure_update.release_status == "PENDING_RELEASE"

    data_structure_update.set_release_status("DRAFT")
    assert data_structure_update.release_status == "DRAFT"

    with pytest.raises(ReleaseStatusException) as e:
        data_structure_update.set_release_status("PENDING_DELETE")
    assert "Can't set release status: PENDING_DELETE" in str(e)
    with pytest.raises(ReleaseStatusException) as e:
        data_structure_update.set_release_status("NO_SUCH_RELEASE_STATUS")
    assert "Invalid release status: NO_SUCH_RELEASE_STATUS" in str(e)

    data_structure_update = DataStructureUpdate(**DATA_STRUCTURE_REMOVE_UPDATE)
    data_structure_update.set_release_status("PENDING_DELETE")
    assert data_structure_update.release_status == "PENDING_DELETE"

    with pytest.raises(ReleaseStatusException) as e:
        data_structure_update.set_release_status("DRAFT")
    assert "Can't set release status: DRAFT" in str(e)

    with pytest.raises(ReleaseStatusException) as e:
        data_structure_update.set_release_status("PENDING_RELEASE")
    assert "Can't set release status: PENDING_RELEASE" in str(e)
