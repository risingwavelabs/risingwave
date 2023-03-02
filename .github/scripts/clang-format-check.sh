sudo apt install clang-format

cd connector_node
clang-format --dry-run -Werror --style="{BasedOnStyle: Google, ReflowComments: false}" proto/**/*.proto\
 || echo "proto format check failed" && exit 1