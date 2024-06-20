{ inputs
, ...
}:

{
  perSystem = { pkgs, lib, ... }: {
    devshells.default =
      let
        rust-toolchain = with pkgs;
          [
            ((rust-bin.fromRustupToolchainFile ../rust-toolchain).override {
              extensions = [ "rust-src" "rust-analyzer" ];
            })
          ] ++ [
            cargo-make
            cargo-nextest
            cargo-binstall
            cargo-sort
            typos
          ];
      in
      {
        imports = [
          "${inputs.devshell}/extra/language/rust.nix"
        ];
        language.rust.enableDefaultToolchain = false;
        packages = rust-toolchain ++ (with pkgs; [
          gcc
          lld
          protobuf
          pkg-config

          gnumake
          cmake
          maven
          jdk17_headless

          tmux
          postgresql
          patchelf
        ]);
        env = [
          {
            name = "PKG_CONFIG_PATH";
            value = lib.concatStringsSep ":" (
              map (pkg: "${pkg}/lib/pkgconfig") (with pkgs; [
                openssl.dev
                cyrus_sasl.dev
              ])
            );
          }
          {
            name = "LD_LIBRARY_PATH";
            value = lib.makeLibraryPath (with pkgs; [
              openssl
              libgcc.lib
            ]);
          }
        ];
      };
  };
}
