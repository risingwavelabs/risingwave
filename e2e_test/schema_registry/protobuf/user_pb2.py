# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: user.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import source_context_pb2 as google_dot_protobuf_dot_source__context__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\nuser.proto\x12\x04test\x1a$google/protobuf/source_context.proto\"\x89\x01\n\x04User\x12\n\n\x02id\x18\x01 \x01(\x05\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x0f\n\x07\x61\x64\x64ress\x18\x03 \x01(\t\x12\x0c\n\x04\x63ity\x18\x04 \x01(\t\x12\x1c\n\x06gender\x18\x05 \x01(\x0e\x32\x0c.test.Gender\x12*\n\x02sc\x18\x06 \x01(\x0b\x32\x1e.google.protobuf.SourceContext\"\xa4\x01\n\x12UserWithMoreFields\x12\n\n\x02id\x18\x01 \x01(\x05\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x0f\n\x07\x61\x64\x64ress\x18\x03 \x01(\t\x12\x0c\n\x04\x63ity\x18\x04 \x01(\t\x12\x1c\n\x06gender\x18\x05 \x01(\x0e\x32\x0c.test.Gender\x12*\n\x02sc\x18\x06 \x01(\x0b\x32\x1e.google.protobuf.SourceContext\x12\x0b\n\x03\x61ge\x18\x07 \x01(\x05\"\x86\x01\n\x0fUserWithNewType\x12\n\n\x02id\x18\x01 \x01(\x05\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x0f\n\x07\x61\x64\x64ress\x18\x03 \x01(\t\x12\x0c\n\x04\x63ity\x18\x04 \x01(\t\x12\x0e\n\x06gender\x18\x05 \x01(\t\x12*\n\x02sc\x18\x06 \x01(\x0b\x32\x1e.google.protobuf.SourceContext*\x1e\n\x06Gender\x12\x08\n\x04MALE\x10\x00\x12\n\n\x06\x46\x45MALE\x10\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'user_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_GENDER']._serialized_start=502
  _globals['_GENDER']._serialized_end=532
  _globals['_USER']._serialized_start=59
  _globals['_USER']._serialized_end=196
  _globals['_USERWITHMOREFIELDS']._serialized_start=199
  _globals['_USERWITHMOREFIELDS']._serialized_end=363
  _globals['_USERWITHNEWTYPE']._serialized_start=366
  _globals['_USERWITHNEWTYPE']._serialized_end=500
# @@protoc_insertion_point(module_scope)
