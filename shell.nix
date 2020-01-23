let 
  moz_overlay = import (builtins.fetchTarball https://github.com/mozilla/nixpkgs-mozilla/archive/master.tar.gz);
  nixpkgs = import <nixpkgs> { overlays = [moz_overlay];};
in
  with nixpkgs;
  stdenv.mkDerivation {
    name = "algebraicdb-dev";
    buildInputs = [ 
      cargo
      pkgconfig
      nix
      rustChannels.stable.rust # replace stable with nighly or beta as needed
      ];
  }