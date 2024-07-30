# Developer Manual

## Code Formatting Manually

We use [Spotless](https://github.com/diffplug/spotless/tree/main/plugin-maven) to format Java code:

```bash
mvn spotless:apply
```

## Set Up Formatter for IDE

### Intellij IDEA

Refer to [Flink's Guild to Set Up IDE](https://nightlies.apache.org/flink/flink-docs-master/docs/flinkdev/ide_setup/).

Install necessary plugins:
- [CheckStyle-IDEA](https://plugins.jetbrains.com/plugin/1065-checkstyle-idea)
- [google-java-format](https://plugins.jetbrains.com/plugin/8527-google-java-format)

Set up Checkstyle plugin
1. Go to “Settings” → “Tools” → “Checkstyle”.
2. Set “Scan Scope” to “Only Java sources (including tests)”.
3. For “Checkstyle Version” select “8.14”.
4. Under “Configuration File” click the “+” icon to add a new configuration.
5. Set “Description” to “RisingWave”.
6. Select “Use a local Checkstyle file” and point it to tools/maven/checkstyle.xml located within your cloned repository.
7. Select “Store relative to project location” and click “Next”.
8. Configure the property checkstyle.suppressions.file with the value suppressions.xml and click “Next”.
9. Click “Finish”.
10 Select “RisingWave” as the only active configuration file and click “Apply”.

Set up google-java-format plugin
1. Go to “Settings” → “Other Settings” → “google-java-format Settings”.
2. Tick the checkbox to enable the plugin.
3. Change the code style to “Android Open Source Project (AOSP) style”.

Import checkstyle configuration to java code formatter
1. Go to “Settings” → “Editor” → “Code Style” → “Java”.
2. Click the gear icon next to “Scheme” and select “Import Scheme” → “Checkstyle Configuration”.
3. Navigate to and select tools/maven/checkstyle.xml located within your cloned repository.

### VS Code (WIP)

Install extension [Checkstyle for Java](https://marketplace.visualstudio.com/items?itemName=shengchen.vscode-checkstyle)

Config with the following. It may work.
```json
{
  "java.checkstyle.configuration": "${workspaceFolder}/tools/maven/checkstyle.xml",
  "java.checkstyle.version": "8.14",
  "java.checkstyle.properties": {
    "checkstyle.suppressions.file": "${workspaceFolder}/tools/maven/suppressions.xml"
  },
  "java.format.settings.url": "https://raw.githubusercontent.com/aosp-mirror/platform_development/master/ide/eclipse/android-formatting.xml",
  "java.format.settings.profile": "Android"
}
```
