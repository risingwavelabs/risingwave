local _ = import 'risedev-template.libsonnet';

{
  default: {
    configFile: 'src/config/example.toml',
    steps: [
      _.metaNode,
      _.computeNode { port: 4588, userManaged: true },
      _.computeNode { port: 4599 },
      _.minio,
      _.frontend,
    ],
  },

  'default-with-monitoring': $.default {
    steps+: [
      _.prometheus,
      _.grafana,
    ],
  },
}
