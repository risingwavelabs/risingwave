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
            cargo-sort
            typos
          ];
      in
      {
        packages =
          rust-toolchain
          ++ (with pkgs; [
            gnumake
            cmake
            protobuf
          ]);
      };
  };
}
