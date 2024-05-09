local _ = import 'risedev-template.libsonnet';

{
  'default-new': {
    configFile: 'src/config/example.toml',
    steps: [
      _.metaNode,
      _.computeNode,
      _.frontend,
    ],
  },

  // 'default-new-with-monitoring': $['default-new'] {
  //   steps+: [
  //     _.prometheus,
  //     _.grafana,
  //   ],
  // },
}
