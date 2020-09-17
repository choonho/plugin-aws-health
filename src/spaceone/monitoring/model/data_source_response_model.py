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


# class PluginOptionsModel(Model):
#     supported_resource_type = ListType(StringType, default=_SUPPORTED_RESOURCE_TYPE)
#     reference_keys = ListType(ModelType(ReferenceKeyModel),
#                               default=[])
#
#
# class PluginVerifyModel(Model):
#     options = ModelType(PluginOptionsModel, default=PluginOptionsModel)
#
#
# class PluginVerifyResponseModel(Model):
#     resource_type = StringType(required=True, default='monitoring.DataSource')
#     actions = ListType(DictType(StringType))
#     result = ModelType(PluginVerifyModel, required=True, default=PluginVerifyModel)
#

class PluginMetadata(Model):
    supported_resource_type = ListType(StringType, default=_SUPPORTED_RESOURCE_TYPE)
    reference_keys = ListType(ModelType(ReferenceKeyModel), default=[])


class PluginInitResponse(Model):
    _metadata = ModelType(PluginMetadata, default=PluginMetadata, serialized_name='metadata')
