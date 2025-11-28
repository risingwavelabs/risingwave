# RisingWave Developer guide

This is the source of the RisingWave Developer guide, published at
<https://risingwavelabs.github.io/risingwave/> (via GitHub Actions to GitHub Pages).
(Note: the url was previously for the crate rustdocs, and now they are moved to <https://risingwavelabs.github.io/risingwave/rustdoc>)

The book is written in Markdown, using the [mdbook](https://rust-lang.github.io/mdBook/) tool to convert to HTML.

Edit `SUMMARY.md` to add new chapters.

## Building the book

```sh
# install tools
> cargo install mdbook mdbook-toc mdbook-linkcheck2
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

## Images

We recommend that you use [draw.io](https://app.diagrams.net/) to draw illustrations and export as SVG images, with "include a copy of my diagram" selected for further editing.
