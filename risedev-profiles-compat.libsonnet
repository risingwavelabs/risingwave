local yaml = importstr 'risedev.yml';
local profiles = std.parseYaml(yaml).profile;

local _ = import 'risedev-template.libsonnet';

local kebabToCamel = function(s)
  std.join('', std.mapWithIndex(
    function(i, x) if i == 0 then x else std.asciiUpper(x[0]) + x[1:]
    , std.split(s, '-')
  ));

local mapStep = function(step)
  _[kebabToCamel(step.use)]
  {
    [kebabToCamel(name)]: step[name]
    for name in std.objectFields(step)
    if name != 'use'
  }
;

local mapProfile = function(name, profile)
  (if 'config-path' in profile then { configPath: profile['config-path'] } else {})
  { steps: std.map(mapStep, profile.steps) };

std.mapWithKey(mapProfile, profiles)
