# RisingWave Developer guide

This is the source of the RisingWave Developer guide, published at
<https://risingwave.github.io/cargo/contrib/>. It is written in Markdown, using
the [mdbook](https://rust-lang.github.io/mdBook/) tool to convert to HTML. If you are editing these pages, the best
option to view the results is to run `mdbook serve`, which will start a web
server on localhost that you can visit to view the book, and it will
automatically reload each time you edit a page.

This is published via GitHub Actions to GitHub Pages.


Edit `SUMMARY.md` to add new chapters.

Install tools:

```
> cargo binstall mdbook mdbook-linkcheck mdbook-toc mdbook-mermaid
```

## Table of Contents

We use `mdbook-toc` to auto-generate TOCs for long sections. You can invoke the preprocessor by
including the `<!-- toc -->` marker at the place where you want the TOC.


### Link Validations

We use `mdbook-linkcheck` to validate URLs included in our documentation.
`linkcheck` will be run automatically when you build with the instructions in the section above.
