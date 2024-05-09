local pruneProvide = function(step)
  {
    [name]: step[name]
    for name in std.objectFields(step)
    if !std.startsWith(name, 'provide')
  };

local mapStep = function(step, steps)
  { use: std.splitLimitR(step.id, '-', 1)[0] }
  {
    [name]: if std.startsWith(name, 'provide') then
      [
        pruneProvide(otherStep)
        for otherStep in steps
        if std.startsWith(otherStep.id, std.rstripChars(step[name], '*'))
      ]
    else step[name]
    for name in std.objectFields(step)
  };

local mapSteps = function(steps)
  [
    mapStep(step, steps)
    for step in steps
  ];

local mapRoot = function(allProfiles)
  std.mapWithKey(
    function(name, profile) profile { steps: mapSteps(profile.steps) },
    allProfiles,
  );

mapRoot
