{
  description = "Library for a distributed simulation system based on the HLA architecture";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    git-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };
  outputs =
    inputs:
    inputs.flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-darwin"
      ];
      imports = [ inputs.git-hooks.flakeModule ];
      perSystem =
        { pkgs, config, ... }:
        {
          formatter = pkgs.nixfmt;
          devShells.default = pkgs.mkShellNoCC {
            shellHook = config.pre-commit.installationScript;
            packages = with pkgs; [
              jdk25_headless
            ];
          };
          pre-commit.settings = {
            package = pkgs.prek;
            hooks = {
              nixfmt.enable = true;
              nil.enable = true;
              statix.enable = true;
              flake-checker.enable = true;
              deadnix.enable = true;
            };
          };
        };
    };
}
