# Introduction

This guide is intended to be used by contributors to learn about how to develop RisingWave. The instructions about how to submit code changes are included in [contribution guidelines](./contributing.md).

If you have questions, you can search for existing discussions or start a new discussion in the [Discussions forum of RisingWave](https://github.com/risingwavelabs/risingwave/discussions), or ask in the RisingWave Community channel on Slack. Please use the [invitation link](https://risingwave.com/slack) to join the channel.

To report bugs, create a [GitHub issue](https://github.com/risingwavelabs/risingwave/issues/new/choose).

Note: the url was previously for the crate rustdocs, and now they are moved to path [`/risingwave/rustdoc`](https://risingwavelabs.github.io/risingwave/rustdoc)

## Read the design docs

<!-- TODO: merge here -->
Before you start to make code changes, ensure that you understand the design and implementation of RisingWave. We recommend that you read the design docs listed in [docs/README.md](https://github.com/risingwavelabs/risingwave/blob/fb60113c2e8a7f0676af545c99f073a335c255f3/docs/README.md) first.

You can also read the [crate level documentation](https://risingwavelabs.github.io/risingwave/rustdoc) for implementation details, or run `./risedev doc` to read it locally.

## Learn about the code structure

<!-- TODO: migrate here -->
- The `src` folder contains all of the kernel components, refer to [src/README.md](https://github.com/risingwavelabs/risingwave/blob/fb60113c2e8a7f0676af545c99f073a335c255f3/src/README.md) for more details, which contains more details about Design Patterns in RisingWave.
- The `docker` folder contains Docker files to build and start RisingWave.
- The `e2e_test` folder contains the latest end-to-end test cases.
- The `docs` folder contains the design docs. If you want to learn about how RisingWave is designed and implemented, check out the design docs here.
- The `dashboard` folder contains RisingWave dashboard.
