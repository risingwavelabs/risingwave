const lintPackages = new Set([
  "eslint-config-next",
  "prettier-plugin-organize-imports",
  "ts-api-utils",
  "ts-declaration-location",
  "typescript-eslint",
])

function readPackage(pkg) {
  const usesLegacyCompiler =
    pkg.name?.startsWith("@typescript-eslint/") || lintPackages.has(pkg.name)

  if (usesLegacyCompiler && pkg.peerDependencies?.typescript) {
    // Isolate consumers of the JavaScript compiler API from Next's native CLI.
    pkg.dependencies = { ...pkg.dependencies, typescript: "5.4.2" }
    delete pkg.peerDependencies.typescript
  }

  return pkg
}

module.exports = { hooks: { readPackage } }
