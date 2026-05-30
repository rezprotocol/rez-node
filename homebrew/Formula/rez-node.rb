class RezNode < Formula
  desc "Rez node runtime CLI"
  homepage "https://github.com/rezprotocol/rez-node"
  url "https://github.com/rezprotocol/rez-node/releases/download/v0.1.0/rez-node-0.1.0-macos-arm64.tar.gz"
  sha256 "REPLACE_WITH_RELEASE_SHA256"
  version "0.1.0"
  license "Apache-2.0"

  def install
    bin.install "rez-node"
  end

  test do
    output = shell_output("#{bin}/rez-node version").strip
    assert_match version.to_s, output
  end
end
