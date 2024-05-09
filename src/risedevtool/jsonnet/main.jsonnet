// Entrypoint for selecting a profile and expanding it.

local expand = import 'expand.libsonnet';

local profiles = import '../../../risedev-profiles.libsonnet';
local compatProfiles = import 'profiles-compat.libsonnet';
local allProfiles = profiles + compatProfiles;

local profile = std.extVar('profile');

if profile in allProfiles then
  expand(allProfiles[profile])
else
  error 'unknown profile ' + profile
