> [!NOTE]
>
> Please write new tests according to the style in `e2e_test/source_inline`.
> Don't add new tests here.
>
> See the [connector development guide](http://risingwavelabs.github.io/risingwave/connector/intro.html#end-to-end-tests) for more information about how to test.

Test in this directory needs some prior setup.

See also `ci/scripts/e2e-source-test.sh`, and `e2e_test/source_legacy/basic/scripts`

## Kafka

`e2e_test/source_legacy/basic/scripts/test_data` contains the data. Filename's convention is `<topic_name>.<n_partitions>`.
