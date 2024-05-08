local expand = import 'expand.libsonnet';
local profiles = import 'risedev-profiles.libsonnet';

expand(profiles)[std.extVar('profile')]
