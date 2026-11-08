{
  description = "agent-bus: cross-host 1-to-1 messaging between Claude Code sessions";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAll = f: nixpkgs.lib.genAttrs systems (s: f nixpkgs.legacyPackages.${s});
    in
    {
      # One binary: broker, shim, wait, hook, hermes-wake. Pure Go (no cgo),
      # matching the justfile's CGO_ENABLED=0 builds.
      packages = forAll (pkgs: rec {
        agent-bus = pkgs.buildGoModule {
          pname = "agent-bus";
          version = self.shortRev or self.dirtyShortRev or "dev";
          src = self;
          vendorHash = "sha256-akUbuvNkSDQV0QJMClY5TKP7ir88d843xsGA3VDBipA=";
          env.CGO_ENABLED = "0";
          subPackages = [ "cmd/agent-bus" ];
          ldflags = [ "-s" "-w" "-X main.version=${self.shortRev or self.dirtyShortRev or "dev"}" ];
          meta.mainProgram = "agent-bus";
        };
        default = agent-bus;
      });
    };
}
