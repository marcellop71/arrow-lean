{
  description = "arrow-lean - Lean 4 bindings for Apache Arrow";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    zlog-lean.url = "github:marcellop71/zlog-lean";
  };

  outputs = { self, nixpkgs, flake-utils, zlog-lean }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        zlogDeps = zlog-lean.lib.${system};
        lean4Bin = pkgs.stdenv.mkDerivation {
          pname = "lean4";
          version = "4.27.0-rc1";
          src = pkgs.fetchurl {
            url = "https://github.com/leanprover/lean4/releases/download/v4.27.0-rc1/lean-4.27.0-rc1-linux.zip";
            sha256 = "64e651f5846a0f4e6e9759a09f5818ae9d16eecf79c157a3bb50968211494a92";
          };
          nativeBuildInputs = [ pkgs.unzip pkgs.autoPatchelfHook ];
          buildInputs = [ pkgs.stdenv.cc.cc.lib pkgs.zlib ];
          installPhase = ''
            mkdir -p $out
            unzip -q $src -d $out
            ln -s $out/lean-4.27.0-rc1-linux/bin $out/bin
          '';
        };
        leanBin = lean4Bin;
        lakeBin = lean4Bin;

        # Native dependencies for building
        nativeDeps = [
          zlogDeps.zlog
          pkgs.gmp
          pkgs.arrow-cpp
        ];

        # GCC library path for libstdc++
        gccLibPath = "${pkgs.stdenv.cc.cc.lib}/lib";

        # Development shell with all dependencies
        devShell = pkgs.mkShell {
          buildInputs = nativeDeps ++ [
            leanBin
            lakeBin
            pkgs.clang
            pkgs.lld
            pkgs.gcc
          ];

          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath (nativeDeps ++ [ pkgs.stdenv.cc.cc.lib ]);
          LIBRARY_PATH = pkgs.lib.makeLibraryPath (nativeDeps ++ [ pkgs.stdenv.cc.cc.lib ]);
          C_INCLUDE_PATH = zlogDeps.zlogInclude;
          CPLUS_INCLUDE_PATH = "${pkgs.arrow-cpp}/include";
          ARROW_LIB_PATH = "${pkgs.arrow-cpp}/lib";
          GCC_LIB_PATH = gccLibPath;

          shellHook = ''
            echo "arrow-lean development environment"
            echo "Lean version: $(lean --version 2>/dev/null || echo 'Lean not found')"
          '';
        };

        # Arrow Lean package build
        arrowLeanPackage = pkgs.stdenv.mkDerivation {
          pname = "arrow-lean";
          version = "0.1.0";
          src = ./.;

          nativeBuildInputs = [
            leanBin
            pkgs.clang
            pkgs.lld
            pkgs.makeWrapper
          ];

          buildInputs = nativeDeps ++ [ zlogDeps.zlogLeanPackage ];

          patchPhase = ''
            # Remove require statements from lakefile.lean for Nix build
            # Dependencies are provided via LEAN_PATH instead
            sed -i '/^require Cli from git/,+1d' lakefile.lean
          '';

          configurePhase = ''
            export HOME=$TMPDIR
            export LEAN_PATH="${zlogDeps.zlogLeanPackage}/lib/lean:$LEAN_PATH"
          '';

          buildPhase = ''
            export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath (nativeDeps ++ [ zlogDeps.zlogLeanPackage ])}"
            export LIBRARY_PATH="${pkgs.lib.makeLibraryPath (nativeDeps ++ [ zlogDeps.zlogLeanPackage ])}"
            export C_INCLUDE_PATH="${zlogDeps.zlogInclude}"
            export CPLUS_INCLUDE_PATH="${pkgs.arrow-cpp}/include"
            export ARROW_LIB_PATH="${pkgs.arrow-cpp}/lib"
            export LEAN_PATH="${zlogDeps.zlogLeanPackage}/lib/lean:$LEAN_PATH"
            ZLOG_LEAN_DIR="$TMPDIR/zlog-lean"
            cp -r "${zlog-lean}" "$ZLOG_LEAN_DIR"
            chmod -R u+w "$ZLOG_LEAN_DIR"
            cat > $TMPDIR/lake-packages.json <<EOF
            {
              "version": "1.1.0",
              "packages": [
                {
                  "type": "path",
                  "scope": "",
                  "name": "zlogLean",
                  "manifestFile": "lake-manifest.json",
                  "inherited": false,
                  "dir": "$ZLOG_LEAN_DIR",
                  "configFile": "lakefile.lean"
                }
              ]
            }
            EOF
            lake build --packages $TMPDIR/lake-packages.json
          '';

          installPhase = ''
            mkdir -p $out/bin $out/lib

            # Copy binaries if they exist
            if [ -d build/bin ]; then
              for bin in build/bin/*; do
                if [ -f "$bin" ]; then
                  cp "$bin" $out/bin/
                  wrapProgram $out/bin/$(basename "$bin") \
                    --set LD_LIBRARY_PATH "${pkgs.lib.makeLibraryPath (nativeDeps ++ [ zlogDeps.zlogLeanPackage ])}"
                fi
              done
            fi

            # Copy library files
            if [ -d build/lib ]; then
              cp -r build/lib/* $out/lib/
            fi

            # Copy metadata files
            mkdir -p $out/lib/lean
            if [ -d build/ir ]; then
              cp -r build/ir $out/lib/lean/
            fi
            cp lakefile.lean $out/lib/lean/
            cp lean-toolchain $out/lib/lean/
          '';
        };

      in {
        devShells.default = devShell;

        packages.default = arrowLeanPackage;

        lib = {
          inherit nativeDeps arrowLeanPackage;
        };
      }
    );
}
