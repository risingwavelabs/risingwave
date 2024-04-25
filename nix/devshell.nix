{
  perSystem = { pkgs, lib, ... }: {
    devshells.default =
      let
        rust-toolchain = with pkgs;
          [
            ((rust-bin.fromRustupToolchainFile ../rust-toolchain).override
              {
                extensions = [ "rust-src" "rust-analyzer" ];
              })
          ] ++ [
            cargo-make
            cargo-nextest
            cargo-binstall
            typos
          ];
      in
      {
        packages =
          rust-toolchain
          ++ (with pkgs; [
            # gcc
            # clang
            # lld
            gnumake
            cmake
            protobuf
          ]);
      };
  };
}
