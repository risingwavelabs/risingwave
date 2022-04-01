# Legacy Frontend

The legacy prototype frontend is in maintenance mode, and should never been used
in production. If you want to develop the legacy frontend, you will need to install
a Java environment.

## Prepare

```shell
# On Debian-based Linux systems
sudo apt install openjdk-11-jdk

# ... or on macOS
brew install java11
```

We only support Java 11 for now.

## Develop

RiseDev can help start a full development cluster with the legacy Java frontend.

Firstly, you will need to enable `Build Java components` option in RiseDev config.

```shell
# enable in one command
./risedev configure enable legacy-frontend

# ... or use the interactive guide to enable features you want
./risedev configure
```

Then, you may start a full development cluster with:

```shell
./risedev d legacy
```
