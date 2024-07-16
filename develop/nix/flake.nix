# A nix flake that sets up a complete RisingWave development environment.
#
# You must have already installed Nix (https://nixos.org) on your system to use this.
# Nix can be installed on Linux or MacOS; NixOS is not required. Windows is not
# directly supported, but Nix can be installed inside of WSL2 or even Docker
# containers. Please refer to https://nixos.org/download for details.
#
# You must also enable support for flakes in Nix. See the following for how to
# do so permanently: https://nixos.wiki/wiki/Flakes#Enable_flakes
#
# Usage:
#
# With Nix installed, navigate to the directory containing this flake and run
# `nix develop ./develop/nix`.
#
# You should now be dropped into a new shell with all programs and dependencies
# available to you!
#
# You can exit the development shell by typing `exit`, or using Ctrl-D.
#
# If you would like this development environment to activate automatically
# upon entering this directory in your terminal, first install `direnv`
# (https://direnv.net/). Then run `echo 'use flake ./develop/nix' >> .envrc` at
# the root of the RisingWave repo. Finally, run `direnv allow` to allow the
# contents of '.envrc' to run every time you enter this directory. Voilà!
#
# note: If you don't want to see git untracked .envrc files bother you,
# you can run `echo '.envrc' >> .git/info/exclude` in the root of project
# to make it ignored locally.

{
  description = ''
    RisingWave the Cloud-native SQL stream processing, analytics, and management.
  '';

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";

    devshell.url = "github:numtide/devshell";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.devshell.flakeModule
        ./overlays.nix
        ./devshell.nix
      ];

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
    };
}

