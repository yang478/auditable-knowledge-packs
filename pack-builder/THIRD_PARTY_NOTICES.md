# Third-Party Notices (Bundled Binaries)

This repository bundles third-party command-line binaries to provide **deterministic, offline exact search** inside generated knowledge-pack skills.

## Bundled Components

### ripgrep (`rg`)

- Version: 15.1.0
- Upstream: https://github.com/BurntSushi/ripgrep
- License: MIT OR The Unlicense
- Included license texts:
  - `third_party/LICENSE-ripgrep-MIT.txt`
  - `third_party/LICENSE-ripgrep-UNLICENSE.txt`

### fd (`fd`)

- Version: 10.3.0
- Upstream: https://github.com/sharkdp/fd
- License: MIT OR Apache-2.0
- Included license texts:
  - `third_party/LICENSE-fd-MIT.txt`
  - `third_party/LICENSE-fd-APACHE-2.0.txt`

## Where They Are Used

- Source binaries are stored at `pack-builder/bin/{rg,fd}`.
- Generated skills copy these binaries to `<skill-root>/bin/{rg,fd}`.
- Generated skills also copy this notice file and the referenced license texts for compliance.

No modifications are made to the upstream binaries.

