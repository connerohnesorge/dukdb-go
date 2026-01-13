{
  description = "A development shell for go";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    treefmt-nix.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    treefmt-nix,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [
          (final: prev: {
            # Add your overlays here
            # Example:
            # my-overlay = final: prev: {
            #   my-package = prev.callPackage ./my-package { };
            # };
            final.buildGoModule = prev.buildGo125Module;
            buildGoModule = prev.buildGo125Module;
          })
        ];
      };

      rooted = exec:
        builtins.concatStringsSep "\n"
        [
          ''REPO_ROOT="$(git rev-parse --show-toplevel)"''
          exec
        ];

      # DuckDB with iceberg extension pre-installed
      # Uses official binary for ABI compatibility with extensions
      duckdb-with-iceberg = pkgs.stdenv.mkDerivation rec {
        pname = "duckdb-with-iceberg";
        version = "1.4.3";

        src = pkgs.fetchurl {
          url = "https://github.com/duckdb/duckdb/releases/download/v${version}/duckdb_cli-linux-amd64.zip";
          sha256 = "0z1gl5ck4i08v887ch9drqjk9311ll3xdvpv9psm78vi38a3d7ks";
        };

        icebergExt = pkgs.fetchurl {
          url = "https://extensions.duckdb.org/v${version}/linux_amd64/iceberg.duckdb_extension.gz";
          sha256 = "1scmyq8swpzhyx1sy448hp98s1f665nhbvrdznw6w41xhpf09nis";
        };

        nativeBuildInputs = [pkgs.unzip];

        unpackPhase = "unzip $src";

        installPhase = ''
          mkdir -p $out/bin $out/extensions/v${version}/linux_amd64
          cp duckdb $out/bin/.duckdb-unwrapped
          chmod +x $out/bin/.duckdb-unwrapped

          gunzip -c $icebergExt > $out/extensions/v${version}/linux_amd64/iceberg.duckdb_extension

          cat > $out/bin/duckdb << WRAPPER
          #!/usr/bin/env bash
          export DUCKDB_EXTENSION_DIRECTORY="$out/extensions"
          exec "$out/bin/.duckdb-unwrapped" "\$@"
          WRAPPER
          chmod +x $out/bin/duckdb
        '';
      };

      scripts = {
        lint = {
          exec = ''
            golangci-lint run
          '';
          description = "Run golangci-lint";
        };
        tests = {
          exec = rooted ''
            gotestsum --format short-verbose "$REPO_ROOT"/...
          '';
          description = "Run tests";
          deps = [pkgs.gotestsum];
        };
      };

      scriptPackages =
        pkgs.lib.mapAttrs
        (
          name: script:
            pkgs.writeShellApplication {
              inherit name;
              text = script.exec;
              runtimeInputs = script.deps or [];
            }
        )
        scripts;

      treefmtModule = {
        projectRootFile = "flake.nix";
        programs = {
          alejandra.enable = true; # Nix formatter
          gofmt.enable = true; # Go formatter
          golines.enable = true; # Go formatter (Shorter lines)
          goimports.enable = true; # Go formatter (Organize/Clean imports)
        };
      };
    in {
      devShells.default = pkgs.mkShell {
        name = "dev";

        # Available packages on https://search.nixos.org/packages
        packages = with pkgs;
          [
            alejandra # Nix
            nixd
            statix
            deadnix

            go_1_25 # Go Tools
            air
            golangci-lint
            gopls
            revive
            golines
            golangci-lint-langserver
            gomarkdoc
            gotests
            gotools
            gotestsum
            reftools
            pprof
            graphviz
            goreleaser
            gofumpt
            duckdb-with-iceberg
          ]
          ++ builtins.attrValues scriptPackages;
      };

      packages = {
        default = pkgs.buildGoModule {
          pname = "my-go-project";
          version = "0.0.1";
          src = self;
          vendorHash = null;
          meta = with pkgs.lib; {
            description = "My Go project";
            homepage = "https://github.com/connerohnesorge/my-go-project";
            license = licenses.asl20;
            maintainers = with maintainers; [connerohnesorge];
          };
        };
      };

      formatter = treefmt-nix.lib.mkWrapper pkgs treefmtModule;
    });
}
