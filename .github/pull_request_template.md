I hereby agree to the terms of the [RisingWave Labs, Inc. Contributor License Agreement](https://gist.github.com/TennyZhuang/f00be7f16996ea48effb049aa7be4d66#file-rw_cla).

## What's changed and what's your intention?

<!--

**This section will be used as the commit message. Please do not leave this empty!**

Please explain **IN DETAIL** what the changes are in this PR and why they are needed:

- Summarize your change (**mandatory**)
- How does this PR work? Need a brief introduction for the changed logic (optional)
- Describe clearly one logical change and avoid lazy messages (optional)
- Describe any limitations of the current code (optional)
- Refer to a related PR or issue link (optional)

-->

## Checklist For Contributors

- [ ] I have written necessary rustdoc comments
- [ ] I have added necessary unit tests and integration tests
- [ ] I have added fuzzing tests or opened an issue to track them. (Optional, recommended for new SQL features #7934).
- [ ] All checks passed in `./risedev check` (or alias, `./risedev c`)

## Checklist For Reviewers

- [ ] I have requested macro/micro-benchmarks as this PR can affect performance substantially, and the results are shown.
<!-- To manually trigger a benchmark, please check out [Notion](https://www.notion.so/risingwave-labs/Manually-trigger-nexmark-performance-dashboard-test-b784f1eae1cf48889b2645d020b6b7d3). -->
- [ ] This PR **IS NOT** a breaking change. 

<details><summary>Click here to link any issues/tests for ensuring backward-compatibility</summary>


## Documentation

- [ ] My PR **DOES NOT** contain user-facing changes.

<!-- 

You can ignore or delete the section below if you ticked the checkbox above.

Otherwise, remove the checkbox above and write a release note below.

-->

<details><summary>Click here for Documentation</summary>

### Types of user-facing changes

Please keep the types that apply to your changes, and remove the others.

- Installation and deployment
- Connector (sources & sinks)
- SQL commands, functions, and operators
- RisingWave cluster configuration changes
- Other (please specify in the release note below)

### Release note

<!--
Please create a release note for your changes. 

Discuss technical details in the "What's changed" section, and 
focus on the impact on users in the release note.

You should also mention the environment or conditions where the impact may occur.
-->

</details>
