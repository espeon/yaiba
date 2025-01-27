# Yaiba: Your Infra Is Basically A CDN Now
Precision engineered static asset delivery.

## Usage

### Running (Development)

```bash
touch sqlite.db && sqlx migrate run # if you don't have a database created
cargo run
```

### Configuration

The following environment variables can be set:

- `MAX_SIZE_BYTES`: The maximum size of the cache in bytes. Defaults to 5GiB.
- `URL_BASE`: The base URL to use for fetching files.

## License

Yaiba is licensed under the MIT license. See [LICENSE](LICENSE.md) for more information.

## Why Yaiba?

Yaiba is a play on Crunchyroll's 'Katana', which stands for "Edge Playbac**k** Orchestr**a**tion for S**ta**teless Multi-CD**N** & SS**A**I"

Yaiba, logically, stands for "**Y**our Infr**a** **I**s **B**asically **A** CDN Now".

The actual japanese word, yaiba (刃), means the blade of a sword (the 'edge' of the sword).

The leap from sword (Katana) to blade (Yaiba) isn’t just poetic – it mirrors the technical shift. Katana orchestrates edge workflows; the goal of Yaiba is to abstract them entirely, treating your gateway as a seamless extension of the CDN. Logical? Sharp, even.

p.s. if the acronym doesn't make you smirk, you're not trying hard enough.
