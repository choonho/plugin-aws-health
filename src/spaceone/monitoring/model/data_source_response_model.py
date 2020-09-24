from schematics.models import Model
from schematics.types import ListType, DictType, StringType
from schematics.types.compound import ModelType

__all__ = ['PluginInitResponse']

_SUPPORTED_RESOURCE_TYPE = [
    'identity.ServiceAccount',
]


class ReferenceKeyModel(Model):
    resource_type = StringType(required=True, choices=_SUPPORTED_RESOURCE_TYPE)
    reference_key = StringType(required=True)


class PluginMetadata(Model):
    supported_resource_type = ListType(StringType, default=_SUPPORTED_RESOURCE_TYPE)
    reference_keys = ListType(ModelType(ReferenceKeyModel), default=[])


class PluginInitResponse(Model):
    _metadata = ModelType(PluginMetadata, default=PluginMetadata, serialized_name='metadata')
