local expand = import 'risedev-expand.libsonnet';

local compatProfiles = import 'risedev-profiles-compat.libsonnet';
local profiles = import 'risedev-profiles.libsonnet';
local allProfiles = profiles + compatProfiles;

local profile = std.extVar('profile');

if profile in allProfiles then
  expand(allProfiles)[profile] { profile: profile }
else
  error 'unknown profile ' + profile
