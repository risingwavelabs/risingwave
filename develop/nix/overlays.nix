{ inputs
, ...
}:

{
  perSystem = { pkgs, system, ... }: {
    _module.args.pkgs = import inputs.nixpkgs {
      inherit system;
      overlays = [
        (import inputs.rust-overlay)
      ];
    };
  };
}
