# RisingWave Developer guide

This is the source of the RisingWave Developer guide, published at
<https://risingwavelabs.github.io/risingwave/> (via GitHub Actions to GitHub Pages).
It is written in Markdown, using the [mdbook](https://rust-lang.github.io/mdBook/) tool to convert to HTML.

Edit `SUMMARY.md` to add new chapters.

## Building the book

```sh
# install tools
> cargo binstall mdbook mdbook-linkcheck mdbook-toc
# start a web server on localhost that you can visit to view the book,
# and it will automatically reload each time you edit a page.
> mdbook serve --open
```

## Table of Contents

We use `mdbook-toc` to auto-generate TOCs for long sections. You can invoke the preprocessor by
including the `<!-- toc -->` marker at the place where you want the TOC.


## Link Validations

We use `mdbook-linkcheck` to validate URLs included in our documentation.
`linkcheck` will be run automatically when you build with the instructions in the section above.
