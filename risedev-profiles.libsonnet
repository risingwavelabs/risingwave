// Profiles for `risedev dev`.
//
// This is the next-generation version of `risedev.yml`. Through `jsonnet`, it's easier for
// developers to reuse and compose profiles.
//
// `risedev.yml` and `risedev-profiles.user.yml` are still supported now, but they will be
// eventually migrated to this file and then deprecated.

local _ = import 'risedev-template.libsonnet';

{
  'jsonnet-example': {
    steps: [
      _.metaNode,
      _.computeNode,
      _.frontend,
    ],
  },

  'jsonnet-example-with-monitoring': $['jsonnet-example'] {
    // Add more steps to the base `jsonnet-example` profile.
    steps+: [
      _.prometheus,
      _.grafana,
    ],
  },

  'jsonnet-example-dev-frontend': {
    // Specify the path to the configuration file.
    configPath: 'src/config/example.toml',
    steps: [
      _.metaNode,
      _.computeNode,
      // Override configurations for the `frontend` step.
      _.frontend { userManaged: true },
    ],
  },
}
