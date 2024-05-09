local expand = import 'risedev-expand.libsonnet';
local allProfiles = import 'risedev-profiles.libsonnet';
local profile = std.extVar('profile');

if profile in allProfiles then
  expand(allProfiles)[profile] { profile: profile }
else
  error 'unknown profile ' + profile
