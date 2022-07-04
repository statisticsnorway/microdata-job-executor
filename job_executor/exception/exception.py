class BuilderStepError(Exception):
    ...


class HttpResponseError(Exception):
    ...


class VersioningException(Exception):
    ...


class ReleaseStatusException(Exception):
    ...


class NoSuchDraftException(Exception):
    ...


class UnknownOperationException(Exception):
    ...


class PatchingError(Exception):
    ...


class MetadataException(Exception):
    ...
