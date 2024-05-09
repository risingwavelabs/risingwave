// Post-process on a profile.

local pruneProvide = function(step)
  {
    [name]: step[name]
    for name in std.objectFields(step)
    if !std.startsWith(name, 'provide')
  };

local mapStep = function(step, steps)
  step
  // Add a kebab-case `use` field to the step from the `id` field, as the tag when deserializing.
  { use: std.splitLimitR(step.id, '-', 1)[0] }
  // Expand all `provide-xx` fields.
  {
    [name]:
      [
        // Expand the provided steps.
        // The nested `provide-xx` fields will not be used. Prune them.
        pruneProvide(otherStep)
        for otherStep in steps
        if std.startsWith(otherStep.id, std.rstripChars(step[name], '*'))
      ]
    for name in std.objectFields(step)
    if std.startsWith(name, 'provide')
  };

local mapSteps = function(steps)
  [
    mapStep(step, steps)
    for step in steps
  ];

local mapProfile = function(profile)
  profile { steps: mapSteps(profile.steps) };

mapProfile
