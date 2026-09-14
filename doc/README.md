# RadixDB documentation

[Русский](README.ru.md)

This directory contains the validated Astro/Starlight manual for RadixDB 1.2.25.
English is the primary language and every user-facing chapter has a Russian
counterpart. Public examples, navigation metadata and validation scripts live
beside the content.

The official project website is [radixdb.org](https://radixdb.org), support is
available at [dev@radixdb.org](mailto:dev@radixdb.org), and development is
sponsored by [Light Soft](http://light-soft.info/).

Accepted performance, storage-footprint, memory-use and reliability reports
are preserved in the [public evidence archive](public/evidence/README.md).
The manual and its build use only public source files and accepted evidence.

## Local Development

Use Node.js 22.12 or later. From this directory:

```sh
npm ci
npm run build
npm run test:editorial
npm run dev
```

Open `/manual/1.2.25/ru/` or `/manual/1.2.25/en/` on the printed local server URL.
`npm run preview` serves the built site, including its search index.
`DOCS_BASE` overrides the deployment prefix. The documentation target is the
1.2.25 release; the application version and source revision are displayed
separately.

To exercise the publication artifact on a non-root prefix:

```sh
DOCS_SITE=https://docs.invalid \
DOCS_ROOT_BASE=/preview/ \
DOCS_BASE=/preview/manual/1.2.25/ \
npm run test:publication
```

`build:publication` requires the same three variables and writes the packaged
site to `.site-artifact/` by default. It includes the versioned manual, search,
sitemap, 404 page and the legacy redirects; it does not deploy the artifact.

External deployment remains a separate operation.

GitHub Pages serves an English/Russian page with the RadixDB logo and a
five-second redirect to [radixdb.org](https://radixdb.org/). It also provides
a direct link for immediate navigation. The full manual remains on the official
website. `npm run build:pages-redirect` creates the standalone Pages artifact
in `../_site/` without installing documentation dependencies. GitHub Pages must
use **GitHub Actions** as its publishing source.
